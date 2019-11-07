package com.oracle.athena.webserver.server;

// For each connection, there can be multiple transactions going on at the same time.
// i.e. this client can be running dozens of requests in parallel to a single
// server.
// There can also be multiple WriteConnection instantiated to talk to different
// target servers.

import com.oracle.athena.webserver.connectionstate.ConnectionState;

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

public class WriteConnection {

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
            System.out.println("Unable to create write threadpool " + io_ex.getMessage());
            return;
        }

        pendingWrites = new LinkedList<>();

        writeClient = null;
    }

    /*
     ** This function opens up the Channel between the Server this WriteConnection is associated
     ** with and the target port that was specified when the WriteConnection was instantiated.
     */
    AsynchronousSocketChannel openWriteConnection(final int writePort, final int timeoutMs) {

        AsynchronousSocketChannel clientChan;

        /*
         ** These two values are only used if this WriteConnection is being used as an initiator and not
         **   the server side.
         */
        tgtWritePort = writePort;
        tgtClientId = writePort - ServerChannelLayer.baseTcpPort;

        try {
            Thread.sleep(1000);
        } catch (InterruptedException int_ex) {
            int_ex.printStackTrace();
        }

        try {
            clientChan = AsynchronousSocketChannel.open(writeCbThreadpool);
        } catch (IOException cex) {
            System.out.println("IOException: " + cex.getMessage());
            return null;
        }

        InetSocketAddress addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), tgtWritePort);

        System.out.println("openWriteConnection client: " + addr);

        try {
            Future<Void> connectResult = clientChan.connect(addr);
            System.out.println("openWriteConnection connect(1)");

            connectResult.get();

            System.out.println("openWriteConnection connect() complete: ");
        } catch (Exception ex) {
            System.out.println("openWriteConnection connect(2) " + ex.getMessage());
            try {
                clientChan.close();
            } catch (IOException io_ex) {
                System.out.println("openWriteConnection connect(3) " + io_ex.getMessage());
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
    void closeWriteConnection() {

        System.out.println("WriteConnection closeWriteConnection() " + connTransactionId);

        /*
         ** Only close the connection if this is an initiator WriteConnection (meaning the tgtWritePort is valid)
         */
        if ((tgtWritePort != INVALID_SOCKET_PORT) && (writeChannel != null)) {

            try {
                writeChannel.close();
            } catch (IOException io_ex) {
                System.out.println("Unable to close connection: " + connTransactionId + " " + io_ex.getMessage());
            }

            writeChannel = null;
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
                System.out.println("Wait for threadpool shutdown timed out: " + connTransactionId);
            }
        } catch (InterruptedException int_ex) {
            System.out.println("Wait for threadpool shutdown failed: " + connTransactionId + " " + int_ex.getMessage());
        }
    }

    public void writeData(WriteCompletion completion) {
        pendingWrites.add(completion);
    }

    public int getClientId() {
        return tgtWritePort;
    }

    long getTransactionId() {
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
    void writeAvailableData() {

        // Pull all the writes off the queue sequentially
        WriteCompletion completion;

        try {
            completion = pendingWrites.remove();
        } catch (NoSuchElementException ex) {
            // Queue is empty so might as well return success
            System.out.println("WriteConnection writeAvailableData() empty queue");
            return;
        }

        int userBufferSize = completion.getRemainingBytesToWrite();
        ByteBuffer buffer = completion.getBuffer();

        System.out.println("WriteConnection writeAvailableData(1) " + userBufferSize);

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
                System.out.println("writeUserBuffer() bytesXfr: " + result);

                System.out.println(String.format("Client: Write Completed in thread %s", Thread.currentThread().getName()));
                if (result == -1) {
                    try {
                        writeChannel.close();
                        writeChannel = null;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    System.out.println("WriteConnection writeUserBuffer(2) bytesWritten -1");
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
                System.out.println("readFromChannel() bytesXfr: " + exc.getMessage());
                try {
                    writeChannel.close();
                    writeChannel = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /*
    ** Test APIs
     */
    public void setClientConnectionState(ConnectionState work) {
        writeClient = work;
    }

    public void closeChannel() {
        writeClient.closeChannel();
        writeClient = null;
    }

}
