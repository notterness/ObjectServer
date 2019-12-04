package com.oracle.athena.webserver.server;

// For each connection, there can be multiple transactions going on at the same time.
// i.e. this client can be running dozens of requests in parallel to a single
// server.
// There can also be multiple WriteConnection instantiated to talk to different
// target servers.

import com.oracle.athena.webserver.connectionstate.ConnectionState;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteConnection implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(WriteConnection.class);

    /*
     ** This is the number of worker threads that are created to handle callbacks for
     ** the write connection.
     */
    private static final int NUM_WRITE_CB_THREADS = 2;

    private static final int INVALID_SOCKET_PORT = -1;

    /*
     ** tgtWritePort is the actual TCP port number
     ** tgtClientId is a way to uniquely identify the target and is zero indexed to the
     **   SeverChannelLayer.baseTcpPort.
     */
    private int tgtWritePort;
    private int tgtClientId;

    private long connTransactionId;

    private Queue<WriteCompletion> pendingWrites;

    private AsynchronousSocketChannel writeChannel;

    private AsynchronousChannelGroup writeCbThreadpool;

    /*
    ** The link to the ConnectionState is used to allow tests to close out the client
    **   side of the channel at different points to exercise the error paths within
    **   the server code.
     */
    ConnectionState writeClient;


    public WriteConnection(final long transactionId) {

        tgtWritePort = INVALID_SOCKET_PORT;
        tgtClientId = INVALID_SOCKET_PORT;

        /*
         ** The connTransactionId is used for logging to track the connection
         */
        connTransactionId = transactionId;

        try {
            writeCbThreadpool = AsynchronousChannelGroup.withFixedThreadPool(NUM_WRITE_CB_THREADS, Executors.defaultThreadFactory());
        } catch (IOException io_ex) {
            LOG.info("Unable to create write threadpool " + io_ex.getMessage());
            return;
        }

        pendingWrites = new LinkedList<>();

        writeClient = null;
    }

    /*
     ** This function opens up the Channel between the Server this WriteConnection is associated
     ** with and the target port that was specified when the WriteConnection was instantiated.
     */
    public AsynchronousSocketChannel open(final int writePort, final int timeoutMs) {

        AsynchronousSocketChannel clientChan;

        /*
         ** These two values are only used if this WriteConnection is being used as an initiator and not
         **   the server side.
         */
        tgtWritePort = writePort;
        tgtClientId = writePort - ServerChannelLayer.BASE_TCP_PORT;
        // TODO CA: Why is there a random sleep here?
        try {
            Thread.sleep(1000);
        } catch (InterruptedException int_ex) {
            int_ex.printStackTrace();
        }

        try {
            clientChan = AsynchronousSocketChannel.open(writeCbThreadpool);
        } catch (IOException cex) {
            LOG.info("IOException: " + cex.getMessage());
            return null;
        }

        InetSocketAddress addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), tgtWritePort);

        LOG.info("WriteConnection[" + connTransactionId + "] openWriteConnection() client: " + addr);

        try {
            Future<Void> connectResult = clientChan.connect(addr);
            LOG.info("openWriteConnection connect(1)");

            connectResult.get();

            LOG.info("WriteConnection[" + connTransactionId + "] openWriteConnection() complete: ");
        } catch (Exception ex) {
            LOG.info("WriteConnection[" + connTransactionId + "] openWriteConnection(2) " + ex.getMessage());
            try {
                clientChan.close();
            } catch (IOException io_ex) {
                LOG.info("WriteConnection[" + connTransactionId + "] openWriteConnection(3) " + io_ex.getMessage());
            }

            clientChan = null;
        }

        writeChannel = clientChan;

        return clientChan;
    }

    /*
     ** This function is used to allow writes to take place on ServerChannelLayer (server side) connections
     **   without going through the connect() path. For server side connections, the SocketChannel is already
     **   present.
     */
    public void assignAsyncWriteChannel(final AsynchronousSocketChannel channel) {
        writeChannel = channel;
    }

    public AsynchronousSocketChannel getWriteChannel() {
        return writeChannel;
    }

    /*
     ** This function is used to close out the Channel and clean it up.
     */
    @Override
    public void close() {
        LOG.info("WriteConnection[" + connTransactionId + "] closeWriteConnection()");

        /*
         ** Only close the connection if this is an initiator WriteConnection (meaning the tgtWritePort is valid)
         */
        if ((tgtWritePort != INVALID_SOCKET_PORT) && (writeChannel != null)) {

            closeChannel();

            tgtWritePort = INVALID_SOCKET_PORT;
            tgtClientId = INVALID_SOCKET_PORT;
        }

        // TODO: Cleanup any pending writes

        /*
         ** Shutdown the AsynchronousChannelGroup and wait for it to cleanup
         */
        writeCbThreadpool.shutdown();

        try {
            boolean shutdown = writeCbThreadpool.awaitTermination(100, TimeUnit.MILLISECONDS);
            if (!shutdown) {
                LOG.info("Wait for threadpool shutdown timed out: " + connTransactionId);
            }
        } catch (InterruptedException int_ex) {
            LOG.info("Wait for threadpool shutdown failed: " + connTransactionId + " " + int_ex.getMessage());
        }
    }

    public boolean writeData(WriteCompletion completion) {
        return pendingWrites.add(completion);
    }

    public int getClientId() {
        return tgtWritePort;
    }

    public long getTransactionId() {
        return connTransactionId;
    }

    /*
     ** This function writes the user data buffer out on the socket. Since there can be multiple
     **   writes queued up, it walks it way through the writes.
     **
     ** TODO: The completion function needs to kick off the next write operation if one is queued. There
     **   probably needs to be a better method than having one thread manage all write, but need to
     **   think about that.
     */
    public void writeAvailableData() {

        // Pull all the writes off the queue sequentially
        WriteCompletion completion;

        try {
            completion = pendingWrites.remove();
        } catch (NoSuchElementException ex) {
            // Queue is empty so might as well return success
            LOG.info("WriteConnection writeAvailableData() empty queue");
            return;
        }

        int userBufferSize = completion.getRemainingBytesToWrite();
        ByteBuffer buffer = completion.getBuffer();

        LOG.info("WriteConnection[" + connTransactionId + "] writeAvailableData() " + userBufferSize);

        writeUserBuffer(buffer, completion);
    }

    /*
     ** This performs the actual write on the channel.
     **
     */
    private void writeUserBuffer(ByteBuffer userData, WriteCompletion completion) {
        final long writeTimeout = 10;

        userData.limit(completion.getRemainingBytesToWrite());
        writeChannel.write(userData, completion, new CompletionHandler<Integer, WriteCompletion>() {

            @Override
            public void completed(final Integer result, final WriteCompletion completion) {
                LOG.info("WriteConnection[" + connTransactionId + "] writeUserBuffer() bytesXfr: " + result);

                if (result == -1) {
                    closeChannel();

                    LOG.info("WriteConnection writeUserBuffer(2) bytesWritten -1");
                    completion.writeCompleted(-1, connTransactionId);
                } else if (result > 0) {
                    // Check if all the data was written or if the write needs to be sent through the
                    // write path again.
                    completion.writeCompleted(result, connTransactionId);
                } else if (result == 0) {
                }
            }

            @Override
            public void failed(final Throwable exc, final WriteCompletion completion) {
                LOG.info("WriteConnection[" + connTransactionId + "] failed(): " + exc.getMessage());
                closeChannel();

                completion.writeCompleted(-1, connTransactionId);
            }
        });
    }

    /*
    TODO CA: Is this method exposed just for test purposes?
    ** This is the common call to close out the write connection channel.
     */
    public synchronized void closeChannel() {
        if (writeChannel != null) {
            try {
                writeChannel.close();
            } catch (IOException e) {
                // FIXME CA: Something more than just printing the stack trace should be done for this sort of method
                e.printStackTrace();
            }
        }
        writeChannel = null;
    }

    /*
     * FIXME: Test APIs should never be part of the production object.
     *    Test APIs
     */
    public void setClientConnectionState(ConnectionState work) {
        writeClient = work;
    }
}
