package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.BuildHttpResult;
import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.*;
import org.eclipse.jetty.http.HttpStatus;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/*
 ** This is used to keep track of the state of the connection for its lifetime.
 **
 ** chan is the socket that the connection is using to transfer data. It is assigned
 **   later and not at instantiation time to allow the ConnectionState objects to be
 **   managed via a pool.
 */
public class ConnectionState {

    private int connStateId;

    private AsynchronousSocketChannel connChan;

    /*
     ** This is the WriteConnection used to write on the server connection
     */
    private WriteConnection writeConn;

    private ConnectionStateEnum overallState;

    /*
     ** The following is the complete information for the HTTP connection
     */
    private CasperHttpInfo casperHttpInfo;

    /*
     ** maxOutstandingBuffers is how many operations can be done in parallel on this
     **   connection. This is to limit one connection from using all the buffers and
     **   all of the processing resources.
     */
    public static final int MAX_OUTSTANDING_BUFFERS = 10;

    /*
     ** bufferStates is used to keep track of all the buffers in flight that are being processed
     */
    private BufferState[] bufferStates;

    private int currentFreeBufferState;

    /*
     ** bufferReadIndex
     */
    private int bufferReadIndex;
    private AtomicInteger outstandingReadCount;

    /*
     ** bufferReadDoneIndex
     */
    private int bufferReadDoneIndex;

    /*
     ** The following is needed to allow the ConnectionState to allocate buffers to send responses
     */
    private BufferState responseBuffer;

    /*
     ** There is an ByteBufferHttpParser per Connection since each parser keeps its own state.
     */
    private ByteBufferHttpParser httpParser;

    /*
     ** The following is used for client sockets reads to know what to callback when data is
     **   read from the socket. It is not used for server side connections.
     */
    private ClientDataReadCallback clientDataReadCallback;

    /*
     **
     */
    private int allocatedHttpBufferCnt;
    private int dataAvailHttpBufferCnt;

    /*
     ** The next four items are associated with the thread that is running the ConnectionState
     **   state machine.
     */
    private ServerWorkerThread workerThread;
    private BufferStatePool bufferStatePool;
    private WriteConnThread writeThread;
    private BuildHttpResult resultBuilder;


    ConnectionState(final int uniqueId) {
        overallState = ConnectionStateEnum.INVALID_STATE;

        bufferStates = new BufferState[MAX_OUTSTANDING_BUFFERS];
        for (int i = 0; i < MAX_OUTSTANDING_BUFFERS; i++) {
            bufferStates[i] = null;
        }

        currentFreeBufferState = 0;
        outstandingReadCount = new AtomicInteger(0);

        connStateId = uniqueId;

        writeConn = null;
        bufferStatePool = null;
        resultBuilder = null;

        clientDataReadCallback = null;

        allocatedHttpBufferCnt = 0;
        dataAvailHttpBufferCnt = 0;
    }

    /*
     **
     */
    void start() {

        casperHttpInfo = new CasperHttpInfo();

        httpParser = new ByteBufferHttpParser(casperHttpInfo);
    }

    public void stateMachine() {
        switch (overallState) {
            case INITIAL_SETUP:
                bufferStatePool = workerThread.getBufferStatePool();
                writeThread = workerThread.getWriteThread();
                resultBuilder = workerThread.getResultBuilder();

                overallState = ConnectionStateEnum.ALLOC_HTTP_BUFFER;
                // Fall through

            case ALLOC_HTTP_BUFFER:
                if (!allocHttpBufferState())
                    break;
            case READ_HTTP_BUFFER:
                readIntoMultipleBuffers();
                break;

            case READ_NEXT_HTTP_BUFFER:
                readIntoBuffer();
                break;

            case PARSE_HTTP_BUFFER:
                // Clean up the values in the ReadState class
                displayBuffer();
                parseHttp();

                setOverallState(ConnectionStateEnum.SEND_XFR_DATA_RESP);
                workerThread.put(this);
                break;

            case SEND_XFR_DATA_RESP:
                // Send the response to the client to request they send data
                sendResponse();
                break;

            case READ_FROM_CHAN:
                setOverallState(ConnectionStateEnum.CONN_FINISHED);
                workerThread.put(this);
                //readIntoMultipleBuffers(work);
                break;
            case READ_WAIT_FOR_DATA:
                break;

            case READ_NEXT_BUFFER:
                readIntoBuffer();
                break;

            case READ_DONE:
                // Clean up the values in the ReadState class
                displayBuffer();
                break;

            case ALLOC_CLIENT_DATA_BUFFER:
                if (allocClientReadBufferState()) {
                    // advance the Connection state
                    setOverallState(ConnectionStateEnum.READ_CLIENT_DATA);
                } else {
                    break;
                }

            case READ_CLIENT_DATA:
                readIntoBuffer();
                break;

            case CLIENT_READ_CB:
                resetBufferReadDoneIndex();
                BufferState readBufferState = getNextReadDoneBufferState();
                if (readBufferState != null) {
                    callClientReadCallback(0, readBufferState.getBuffer());
                } else {
                    System.out.println("ServerWorkerThread(" + connStateId + ") readBufferState null");
                }

                // TODO: Need to release the BufferState
                break;

            case CONN_FINISHED:
                System.out.println("ServerWorkerThread(" + connStateId + ") CONN_FINISHED");
                reset();

                // FIXME: Need to put the ConnectionState object back on the free pool
                break;
        }
    }

    /*
     ** Allocate a buffer to read data into and associate it with this ConnectionState
     */
    private boolean allocHttpBufferState() {
        BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_FROM_CHAN, MemoryManager.SMALL_BUFFER_SIZE);
        if (bufferState != null) {
            if (addBufferState(bufferState)) {
                // advance the Connection state
                setOverallState(ConnectionStateEnum.READ_HTTP_BUFFER);

                allocatedHttpBufferCnt++;
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    private boolean allocClientReadBufferState() {
        System.out.println("ServerWorkerThread(" + connStateId + ") allocClientReadBufferState(1) " + Thread.currentThread().getName());

        BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_FROM_CHAN, MemoryManager.MEDIUM_BUFFER_SIZE);
        if (bufferState != null) {
            System.out.println("ServerWorkerThread(" + connStateId + ") allocClientReadBufferState(2) " + Thread.currentThread().getName());

            return addBufferState(bufferState);
        }

        return false;
    }


    /*
     ** This is used to start reads into one or more buffers. It looks for BufferState objects that have
     **   their state set to READ_FROM_CHAN. It then sends those buffers off to perform asynchronous reads.
     */
    private void readIntoMultipleBuffers() {
        BufferState buffState;

        /*
         ** Only setup reads for allocated buffers
         */
        if (allocatedHttpBufferCnt > 0) {
            resetBufferReadIndex();
            do {
                buffState = getNextReadBufferState();

                if (buffState != null) {
                    allocatedHttpBufferCnt--;
                    readFromChannel(buffState);
                }
            } while (buffState != null);
        }
    }

    private void readIntoBuffer() {
        BufferState buffState;

        resetBufferReadIndex();

        buffState = getNextReadBufferState();

        if (buffState != null) {
            readFromChannel(buffState);
        } else {
            System.out.println("readIntoBuffer(" + connStateId + ") buffState null");
        }
    }


    void setChannelAndWrite(final AsynchronousSocketChannel chan) {
        connChan = chan;
        overallState = ConnectionStateEnum.INITIAL_SETUP;

        /*
         ** Setup the WriteConnection at this point
         */
        writeConn = new WriteConnection(connStateId);

        writeConn.assignAsyncWriteChannel(chan);
    }

    /*
     ** This only sets up the Connection for reading data for a client.
     */
    void setChannel(final AsynchronousSocketChannel chan) {
        connChan = chan;
        overallState = ConnectionStateEnum.INITIAL_SETUP;
    }

    public void assignWorkerThread(final ServerWorkerThread thread) {
        workerThread = thread;
    }

    /*
     ** This is called when the Server Connection is closed and this tracking object needs
     **   to be cleaned up.
     */
    void clearChannel() {
        connChan = null;

        /* Also clear out the WriteConnection reference */
        writeConn = null;

        bufferStatePool = null;

        clientDataReadCallback = null;
    }

    public AsynchronousSocketChannel getChannel() {
        return connChan;
    }

    public void setOverallState(final ConnectionStateEnum state) {
        overallState = state;
    }

    public ConnectionStateEnum getState() {
        return overallState;
    }

    void setClientReadCallback(ClientDataReadCallback clientReadCb) {
        clientDataReadCallback = clientReadCb;
    }

    public void callClientReadCallback(final int status, final ByteBuffer readBuffer) {
        clientDataReadCallback.dataBufferRead(0, readBuffer);
    }

    public void reset() {
        /*
         ** Release all the memory back to the pool
         */
        for (int i = 0; i < MAX_OUTSTANDING_BUFFERS; i++) {
            if (bufferStates[i] != null) {
                bufferStates[i] = null;
            }
        }

        connChan = null;
        overallState = ConnectionStateEnum.INVALID_STATE;

        /*
         ** Clear the items associated with a particular worker thread that was used to execute this
         **   ConnectionState as it may change the next time this ConnectionState is used.
         */
        workerThread = null;
        writeThread = null;
        bufferStatePool = null;
        resultBuilder = null;

        /*
         ** Clear the write connection (it may already be null) since it will not be valid with the next
         **   use of this ConnectionState
         */
        writeConn = null;

        httpParser.resetHttpParser();
    }

    public boolean addBufferState(BufferState bufferState) {
        if (currentFreeBufferState < MAX_OUTSTANDING_BUFFERS) {
            bufferStates[currentFreeBufferState] = bufferState;
            currentFreeBufferState++;

            return true;
        }

        return false;
    }

    public void resetBufferReadIndex() {
        int readCount = outstandingReadCount.get();
        if (readCount != 0) {
            System.out.println("outstandingReadCount != 0: " + readCount);
        }
        bufferReadIndex = 0;
    }

    /*
     ** This is used to find the next available BufferState that is ready to have a read performed.
     **   If one is found, the outstandingReadCount is incremented. The outstandingReadCount is used
     **   to determine when a ConnectionState can be cleaned up.
     */
    private BufferState getNextReadBufferState() {
        BufferState buffState = null;
        while (bufferReadIndex < MAX_OUTSTANDING_BUFFERS) {
            buffState = bufferStates[bufferReadIndex];

            bufferReadIndex++;

            if (buffState != null) {
                if (buffState.getBufferState() == BufferStateEnum.READ_FROM_CHAN) {
                    outstandingReadCount.incrementAndGet();
                    break;
                } else {
                    buffState = null;
                }
            }
        }

        return buffState;
    }

    public void resetBufferReadDoneIndex() {
        bufferReadDoneIndex = 0;
    }

    /*
     ** This walks through the array of BufferState to find the next one that has completed
     **   the read of data into it and returns it.
     ** NOTE: The expectation is that when the Java NIO.2 read operation performs it's callback,
     **   there is no more data to be transferred into the buffer being read into.
     */
    public BufferState getNextReadDoneBufferState() {
        BufferState buffState = null;
        while (bufferReadDoneIndex < MAX_OUTSTANDING_BUFFERS) {
            buffState = bufferStates[bufferReadDoneIndex];

            bufferReadDoneIndex++;
            if (bufferReadDoneIndex == MAX_OUTSTANDING_BUFFERS) {
                bufferReadDoneIndex = 0;
            }

            if (buffState != null) {
                if (buffState.getBufferState() == BufferStateEnum.READ_DONE) {
                    break;
                } else {
                    buffState = null;
                }
            }
        }

        return buffState;
    }

    /*
     ** This is called when a buffer read was completed. This means that the particular buffer
     **   can be processed by the HTTP Parser at this point.
     **
     ** The call path to get here is:
     **    ServerWorkerThread chan.read() completed() callback
     **      --> BufferState.setReadState()
     **
     ** NOTE: This is only called for the good path for reads. The error path is handled in the
     **   readCompletedError() function.
     */
    void readCompleted(boolean lastRead) {
        int readCount = outstandingReadCount.decrementAndGet();

        System.out.println("ConnectionState.readComplete() lastRead: " + lastRead + " readCount: " + readCount +
                " overallState: " + overallState);

        if (lastRead == false) {
            if (overallState == ConnectionStateEnum.READ_HTTP_BUFFER) {
                overallState = ConnectionStateEnum.READ_NEXT_HTTP_BUFFER;
            } else {
                overallState = ConnectionStateEnum.READ_NEXT_BUFFER;
            }
            workerThread.put(this);
            return;
        }

        /*
         ** If all of the outstanding reads have completed, then advance the state and
         ** queue this work item back up.
         */
        if (readCount == 0) {
            if ((overallState == ConnectionStateEnum.READ_HTTP_BUFFER) || (overallState == ConnectionStateEnum.READ_NEXT_HTTP_BUFFER)) {
                overallState = ConnectionStateEnum.PARSE_HTTP_BUFFER;
            } else if (overallState == ConnectionStateEnum.READ_CLIENT_DATA) {
                overallState = ConnectionStateEnum.CLIENT_READ_CB;
            } else {
                overallState = ConnectionStateEnum.READ_DONE;
            }

            System.out.println("ConnectionState.readComplete() overallState: " + overallState.toString());
            workerThread.put(this);
        }
    }

    /*
     ** The following is used to handle the case when a read error occurred and the ConnectionState needs
     **   to cleanup and release resources.
     **
     ** The call path to get here is:
     **    ServerWorkerThread chan.read() failed() callback
     **      --> BufferState.setReadState()
     **
     */
    void readCompletedError() {
        int readCount = outstandingReadCount.decrementAndGet();

        System.out.println("ConnectionState.readComplete()  readCount: " + readCount +
                " overallState: " + overallState);

        /*
         ** If there are outstanding reads in progress, need to wait for those to
         **   complete before cleaning up the ConnectionState
         */
        int readsInProgress = outstandingReadCount.get();
        if (readsInProgress == 0) {
            setOverallState(ConnectionStateEnum.READ_DONE);
            workerThread.put(this);
        }
    }

    public void displayBuffer() {
        int bufferStateIndex = 0;
        ByteBuffer buffer;

        while (bufferStateIndex < MAX_OUTSTANDING_BUFFERS) {
            BufferState buffState = bufferStates[bufferStateIndex];
            if (buffState != null) {
                buffer = buffState.getCheckedBuffer(BufferStateEnum.READ_DONE);

                if (buffer != null) {
                    System.out.println("buffer[" + bufferStateIndex + "] " + buffer.position() + " " + buffer.limit());

                    String tmp = bb_to_str(buffer);

                    System.out.println("ConnectionState buffer[" + bufferStateIndex + "]" + tmp);
                }
            }

            bufferStateIndex++;
        }
    }

    public void parseHttp() {
        int bufferStateIndex = 0;
        boolean initialBuffer = true;
        ByteBuffer buffer;

        while (bufferStateIndex < MAX_OUTSTANDING_BUFFERS) {
            BufferState buffState = bufferStates[bufferStateIndex];
            if (buffState != null) {
                buffer = buffState.getCheckedBuffer(BufferStateEnum.READ_DONE);

                if (buffer != null) {
                    int bufferTextCapacity = buffer.position();
                    buffer.rewind();
                    buffer.limit(bufferTextCapacity);

                    httpParser.parseHttpData(buffer, initialBuffer);

                    initialBuffer = false;
                }
            }

            bufferStateIndex++;
        }

    }

    public int getConnStateId() {
        return connStateId;
    }

    /*
     ** Sets up the WriteConnection, but only does it once
     */
    public void setupWriteConnection() {
        if (writeConn == null) {
            writeConn = new WriteConnection(1);

            writeConn.assignAsyncWriteChannel(connChan);
        }
    }

    public WriteConnection getWriteConnection() {
        return writeConn;
    }

    public BufferState allocateResponseBuffer() {
        BufferState respBuffer = bufferStatePool.allocBufferState(this, BufferStateEnum.SEND_GET_DATA_RESPONSE, MemoryManager.MEDIUM_BUFFER_SIZE);

        responseBuffer = respBuffer;

        return respBuffer;
    }

    public void statusWriteCompleted(final int bytesXfr, final ByteBuffer buffer) {

        BufferStateEnum currState;

        if (responseBuffer == null) {
            System.out.println("statusWriteCompleted: null responseBuffer" + connStateId);
        }

        currState = responseBuffer.getBufferState();
        System.out.println("statusWriteCompleted: " + connStateId + " " + currState.toString());

        switch (currState) {
            case SEND_GET_DATA_RESPONSE:
                // Advance the ConnectionState's state to reading in the user data
                if (overallState == ConnectionStateEnum.SEND_XFR_DATA_RESP) {
                    // Safety check
                    overallState = ConnectionStateEnum.READ_FROM_CHAN;
                }
                break;

            case SEND_FINAL_RESPONSE:
                break;
        }

        bufferStatePool.freeBufferState(responseBuffer);
        responseBuffer = null;
    }

    public void str_to_bb(ByteBuffer out, String in) {
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();

        try {
            boolean endOfInput = true;

            encoder.encode(CharBuffer.wrap(in), out, endOfInput);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void charbuffer_to_bb(ByteBuffer out, CharBuffer in) {
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();

        try {
            boolean endOfInput = true;

            encoder.encode(in, out, endOfInput);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public String bb_to_str(ByteBuffer buffer) {
        int bufferTextCapacity = buffer.position();
        buffer.rewind();
        buffer.limit(bufferTextCapacity);
        /*
        CharBuffer cbuf = buffer.asCharBuffer();
        String tmp = cbuf.toString();
        */

        String tmp = StandardCharsets.UTF_8.decode(buffer).toString();

        return tmp;
    }

    /*
     ** This is what performs the actual read from the channel.
     */
    private void readFromChannel(final BufferState readBufferState) {

        AsynchronousSocketChannel chan = getChannel();

        try {
            SocketAddress addr = chan.getLocalAddress();

            System.out.println("readFromChannel(" + connStateId + "): socket: " + addr);
        } catch (IOException ex) {
            System.out.println("socket closed " + ex.getMessage());

            try {
                chan.close();
            } catch (IOException io_ex) {
                System.out.println("socket closed " + io_ex.getMessage());
            }

            /*
             ** Mark the BufferState as READ_ERROR to indicate that the buffer is not valid and this
             **   will call into the ConnectionState to move the cleanup of the connection along if
             **   there are no more outstanding reads.
             */
            readBufferState.setReadState(BufferStateEnum.READ_ERROR);
        }

        // Read the data from the channel
        ByteBuffer readBuffer = readBufferState.getBuffer();
        chan.read(readBuffer, readBufferState, new CompletionHandler<Integer, BufferState>() {

            @Override
            public void completed(final Integer bytesRead, final BufferState readBufferState) {
                System.out.println("readFromChannel(" + connStateId + ") bytesRead: " + bytesRead + " thread: " + Thread.currentThread().getName());

                if (bytesRead == -1) {
                    try {
                        getChannel().close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    readBufferState.setReadState(BufferStateEnum.READ_ERROR);
                } else if (bytesRead > 0) {
                    ByteBuffer buffer = readBufferState.getBuffer();
                    if (buffer.remaining() == 0) {
                        // Full buffer so there is likely more data to read
                        allocClientReadBufferState();
                    }
                    // TODO: Need to check if all the data was read in
                    readBufferState.setReadState(BufferStateEnum.READ_DONE);
                } else if (bytesRead == 0) {
                    readBufferState.setReadState(BufferStateEnum.READ_DONE);
                }
            }

            @Override
            public void failed(final Throwable exc, final BufferState readBufferState) {
                System.out.println("readFromChannel(" + connStateId + ") bytesRead: " + exc.getMessage() + " thread: " + Thread.currentThread().getName());

                try {
                    getChannel().close();
                } catch (IOException io_ex) {
                    System.out.println("Read() socket closed " + io_ex.getMessage());
                }

                /*
                 ** This buffer read failed, need to mark the buffer to indicate that it is not valid
                 */
                readBufferState.setReadState(BufferStateEnum.READ_ERROR);
            }
        });  // end of chan.read()
    }

    /*
     ** This will send out a particular response type on the server channel
     */
    private void sendResponse() {

        // Allocate the Completion object specific to this operation
        setupWriteConnection();

        BufferState buffState = allocateResponseBuffer();
        if (buffState != null) {
            ByteBuffer respBuffer = resultBuilder.buildResponse(buffState, HttpStatus.OK_200, true);
            //ByteBuffer respBuffer = resultBuilder.buildResponse(buffState, HttpStatus.CONTINUE_100, false);

            int bytesToWrite = respBuffer.position();
            respBuffer.flip();

            WriteConnection writeConn = getWriteConnection();
            StatusWriteCompletion statusComp = new StatusWriteCompletion(this, writeConn, respBuffer,
                    getConnStateId(), bytesToWrite, 0);
            writeThread.writeData(writeConn, statusComp);

            System.out.println("sendResponse(" + connStateId + ") 2");
        } else {
            System.out.println("sendResponse(" + connStateId + "): unable to allocate response buffer");
        }
    }


}
