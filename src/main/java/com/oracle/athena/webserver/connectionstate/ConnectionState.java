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
import java.util.concurrent.atomic.AtomicBoolean;
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

    private AtomicInteger httpBufferReadsCompleted;
    private boolean initialHttpBuffer;

    private AtomicBoolean httpHeaderParsed;


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

        httpBufferReadsCompleted = new AtomicInteger(0);
        initialHttpBuffer = true;

        httpHeaderParsed = new AtomicBoolean(false);
    }

    /*
     **
     */
    void start() {

        /*
         ** The CasperHttpInfo keeps track of the details of a particular
         **   HTTP transfer and the parsed information.
         */
        casperHttpInfo = new CasperHttpInfo(this);

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
                /*
                 ** Do not continue if there are no buffers allocated
                 */
                if (allocHttpBufferState() == 0)
                    break;
            case READ_HTTP_BUFFER:
                readIntoMultipleBuffers();
                break;

            case PARSE_HTTP_BUFFER:
                parseHttp();

                boolean headerReady = httpHeaderParsed.get();

                if (headerReady) {
                    setOverallState(ConnectionStateEnum.SEND_XFR_DATA_RESP);
                    workerThread.put(this);
                }
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
    private int allocHttpBufferState() {
        BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_FROM_CHAN, MemoryManager.SMALL_BUFFER_SIZE);
        if (bufferState != null) {
            if (addBufferState(bufferState)) {
                // advance the Connection state
                setOverallState(ConnectionStateEnum.READ_HTTP_BUFFER);

                allocatedHttpBufferCnt++;
            }
        }

        return allocatedHttpBufferCnt;
    }

    private boolean allocClientReadBufferState() {
        System.out.println("ServerWorkerThread(" + connStateId + ") allocClientReadBufferState(1) " + Thread.currentThread().getName());

        BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_FROM_CHAN, MemoryManager.SMALL_BUFFER_SIZE);
        if (bufferState != null) {
            System.out.println("ServerWorkerThread(" + connStateId + ") allocClientReadBufferState(2)");

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

    /*
     ** This finds the first available BufferState with its state set to READ_FROM_CHAN
     **   and then performs a read using it.
     */
    private void readIntoBuffer() {
        BufferState buffState;

        resetBufferReadIndex();

        buffState = getNextReadBufferState();

        if (buffState != null) {
            allocatedHttpBufferCnt--;
            readFromChannel(buffState);
        } else {
            System.out.println("readIntoBuffer(" + connStateId + ") buffState null");
        }
    }


    /*
     ** This is used to setup the state machine and associated information to allow
     **   a write to take place on the passed in AsynchronousSocketChannel.
     */
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

    /*
     ** This is called when the WorkerThread that this ConnectionState will execute on is
     **   determined. It is done prior to the ConnectionState being queued to the
     **   WorkerThread the first time.
     **
     ** NOTE: The ConnectionState is reused and may execute on different threads for each
     **   lifetime of a connection.
     */
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

        httpHeaderParsed.set(false);
    }

    /*
     ** TODO: The overallState modifications and reads need to be made thread safe.
     */
    public void setOverallState(final ConnectionStateEnum state) {
        overallState = state;
    }

    public ConnectionStateEnum getState() {
        return overallState;
    }

    void setClientReadCallback(ClientDataReadCallback clientReadCb) {
        clientDataReadCallback = clientReadCb;
    }

    private void callClientReadCallback(final int status, final ByteBuffer readBuffer) {
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

    /*
     ** TODO: Need to handle the wrap case for the array.
     */
    private boolean addBufferState(BufferState bufferState) {
        if (currentFreeBufferState < MAX_OUTSTANDING_BUFFERS) {
            bufferStates[currentFreeBufferState] = bufferState;
            currentFreeBufferState++;

            return true;
        }

        return false;
    }

    /*
     ** TODO: This is probably not needed as the BufferState array can have multiple buffers
     **   on it in different states. Might need something besides an array to keep track of
     **   the buffer states and what they are supposed to be used for. Maybe a FIFO...
     */
    private void resetBufferReadIndex() {
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

    private void resetBufferReadDoneIndex() {
        bufferReadDoneIndex = 0;
    }

    /*
     ** This walks through the array of BufferState to find the next one that has completed
     **   the read of data into it and returns it.
     ** NOTE: The expectation is that when the Java NIO.2 read operation performs it's callback,
     **   there is no more data to be transferred into the buffer being read into.
     */
    private BufferState getNextReadDoneBufferState() {
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

        int readCompletedCount = httpBufferReadsCompleted.incrementAndGet();

        System.out.println("ConnectionState.readComplete() lastRead: " + lastRead + " readCount: " + readCount +
                " overallState: " + overallState);

        if (!lastRead) {
            if (overallState == ConnectionStateEnum.READ_HTTP_BUFFER) {
                overallState = ConnectionStateEnum.ALLOC_HTTP_BUFFER;
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

    /*
     ** This function walks through all the buffers that have reads completed and pushes
     **   then through the HTTP Parser.
     ** Once the header buffers have been parsed, they can be released. The goal is to not
     **   recycle the data buffers, so those may not need to be sent through the HTTP Parser'
     **   and instead should be handled directly.
     */
    private void parseHttp() {
        int bufferStateIndex = 0;
        ByteBuffer buffer;

        int bufferReadsDone = httpBufferReadsCompleted.get();
        if (bufferReadsDone > 0) {
            while (bufferStateIndex < MAX_OUTSTANDING_BUFFERS) {
                BufferState buffState = bufferStates[bufferStateIndex];
                if (buffState != null) {
                    buffer = buffState.getCheckedBuffer(BufferStateEnum.READ_DONE);

                    if (buffer != null) {
                        httpBufferReadsCompleted.decrementAndGet();

                        int bufferTextCapacity = buffer.position();
                        buffer.rewind();
                        buffer.limit(bufferTextCapacity);

                        httpParser.parseHttpData(buffer, initialHttpBuffer);

                        /*
                         ** Set the BufferState to PARSE_DONE
                         ** TODO: When can the buffers used for the header be released?
                         */
                        buffState.setHttpParseDone();

                        initialHttpBuffer = false;
                    }
                }

                bufferStateIndex++;
            }
        }
    }

    /*
     ** This is used to tell the connection management that the HTTP header has been parsed
     **   and the validation and authorization can take place.
     */
    void httpHeaderParseComplete() {
        System.out.println("httpHeaderParseComplete(" + connStateId + ") curr: " + overallState);

        httpHeaderParsed.set(true);
    }

    public int getConnStateId() {
        return connStateId;
    }

    /*
     ** Sets up the WriteConnection, but only does it once
     */
    private void setupWriteConnection() {
        if (writeConn == null) {
            writeConn = new WriteConnection(1);

            writeConn.assignAsyncWriteChannel(connChan);
        }
    }

    private WriteConnection getWriteConnection() {
        return writeConn;
    }

    private BufferState allocateResponseBuffer() {
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

    /*
     ** This is what performs the actual read from the channel.
     */
    private void readFromChannel(final BufferState readBufferState) {

        try {
            SocketAddress addr = connChan.getLocalAddress();

            System.out.println("readFromChannel(" + connStateId + "): socket: " + addr);
        } catch (IOException ex) {
            System.out.println("socket closed " + ex.getMessage());

            try {
                connChan.close();
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
        connChan.read(readBuffer, readBufferState, new CompletionHandler<Integer, BufferState>() {

            @Override
            public void completed(final Integer bytesRead, final BufferState readBufferState) {
                System.out.println("readFromChannel(" + connStateId + ") bytesRead: " + bytesRead + " thread: " + Thread.currentThread().getName());

                if (bytesRead == -1) {
                    try {
                        connChan.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    readBufferState.setReadState(BufferStateEnum.READ_ERROR);
                } else if (bytesRead > 0) {
                    readBufferState.setReadState(BufferStateEnum.READ_DONE);
                } else if (bytesRead == 0) {
                    readBufferState.setReadState(BufferStateEnum.READ_ERROR);
                }
            }

            @Override
            public void failed(final Throwable exc, final BufferState readBufferState) {
                System.out.println("readFromChannel(" + connStateId + ") bytesRead: " + exc.getMessage() + " thread: " + Thread.currentThread().getName());

                try {
                    connChan.close();
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

    /*
     ** DEBUG FUNCTIONS BELOW
     */

    /*
     ** This is a general debug function that dumps the buffer to the console.
     */
    private void displayBuffer() {
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

    private void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            boolean endOfInput = true;

            encoder.encode(CharBuffer.wrap(in), out, endOfInput);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void charbuffer_to_bb(ByteBuffer out, CharBuffer in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(in, out, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private String bb_to_str(ByteBuffer buffer) {
        buffer.flip();

        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

}
