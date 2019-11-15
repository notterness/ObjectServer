package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.StatusWriteCompletion;
import com.oracle.athena.webserver.server.WriteConnection;
import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class WebServerConnState extends ConnectionState {

    /*
     ** The following is needed to allow the ConnectionState to allocate buffers to send responses.
     **
     ** dataResponseSent is set to true when the HTTP response header has been sent.
     */
    private BufferState responseBuffer;
    private AtomicBoolean dataResponseSent;
    private boolean finalResponseSent;
    private AtomicBoolean finalResponseSendDone;

    /*
     **
     */
    private int requestedHttpBuffers;
    private int allocatedHttpBufferCnt;

    private AtomicInteger outstandingHttpReadCount;

    private LinkedList<BufferState> allocatedHttpBufferQueue;

    private AtomicInteger httpBufferReadsCompleted;
    private boolean initialHttpBuffer;
    private BlockingQueue<BufferState> httpReadDoneQueue;

    private AtomicBoolean httpHeaderParsed;

    /*
     ** The following is the complete information for the HTTP connection
     */
    private CasperHttpInfo casperHttpInfo;

    /*
     ** There is an ByteBufferHttpParser per Connection since each parser keeps its own state.
     */
    private ByteBufferHttpParser httpParser;

    /*
     ** This is the WriteConnection used to write responses and return data on the server connection
     */
    private WriteConnection writeConn;

    /*
     ** The following is used to release this ConnectionState back to the free pool.
     */
    private ConnectionStatePool<WebServerConnState> connectionStatePool;


    public WebServerConnState(final ConnectionStatePool<WebServerConnState> pool, final int uniqueId) {

        super(uniqueId);

        connectionStatePool = pool;

        responseBuffer = null;
        dataResponseSent = new AtomicBoolean(false);
        finalResponseSent = false;
        finalResponseSendDone = new AtomicBoolean( false);

        outstandingHttpReadCount = new AtomicInteger(0);
        requestedHttpBuffers = 0;
        allocatedHttpBufferCnt = 0;

        allocatedHttpBufferQueue = new LinkedList<>();

        httpBufferReadsCompleted = new AtomicInteger(0);
        initialHttpBuffer = true;
        httpReadDoneQueue = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);

        httpHeaderParsed = new AtomicBoolean(false);

        writeConn = null;
    }


    public void start() {
        super.start();

        /*
         ** The CasperHttpInfo keeps track of the details of a particular
         **   HTTP transfer and the parsed information.
         */
        casperHttpInfo = new CasperHttpInfo(this);

        httpParser = new ByteBufferHttpParser(casperHttpInfo);
    }

    @Override
    public void stateMachine() {

        /*
         ** First determine the state to execute
         */
        if (overallState != ConnectionStateEnum.INITIAL_SETUP) {
            overallState = determineNextState();
            System.out.println("WebServerConnState[" + connStateId + "] state: " + overallState.toString());
        }

        switch (overallState) {
            case INITIAL_SETUP:
                setupInitial();

                System.out.println("WebServerConnState[" + connStateId + "] INITIAL_SETUP server");
                requestedHttpBuffers = 1;

                /*
                ** Have to advance past INITIAL_SETUP to prevent the state machine from getting stuck
                 */
                overallState = ConnectionStateEnum.CHECK_SLOW_CHANNEL;
                addToWorkQueue(false);
                break;

                // Fall through
            case CHECK_SLOW_CHANNEL:
                if (timeoutChecker.inactivityThresholdReached()) {
                    /*
                     ** TOTDO: Need to close out the channel and this connection
                     */
                } else {
                    overallState = determineNextState();

                    /*
                     ** Need to wait for something to kick the state machine to a new state
                     **
                     ** The ConnectionState will get put back on the execution queue when an external
                     **   operation completes.
                     */
                    if (overallState != ConnectionStateEnum.CHECK_SLOW_CHANNEL) {
                        addToWorkQueue(false);
                    } else {
                        addToWorkQueue(true);
                    }
                }
                break;

            case ALLOC_HTTP_BUFFER:
                /*
                 ** Do not continue if there are no buffers allocated. allocHttpBufferState() returns
                 **   the number of buffers that were allocated.
                 */
                if (allocHttpBufferState() == 0) {
                    /*
                     ** There may be other work that can be done while waiting to allocate buffers
                     */
                    addToWorkQueue(false);
                    break;
                } else {
                    // Fall through
                    overallState = ConnectionStateEnum.READ_HTTP_BUFFER;
                }

            case READ_HTTP_BUFFER:
                readIntoMultipleBuffers();
                addToWorkQueue(false);
                break;

            case PARSE_HTTP_BUFFER:
                parseHttp();

                addToWorkQueue(false);
                break;

            case SEND_XFR_DATA_RESP:
                // Send the response to the client to request they send data
                sendResponse();
                break;

            case READ_FROM_CHAN:
                addToWorkQueue(false);
                break;

            case READ_DONE:
                // TODO: Assert() if this state is ever reached
                break;

            case READ_DONE_ERROR:
                // Release all the outstanding buffer
                releaseBufferState();
                addToWorkQueue(false);
                break;

            case ALLOC_CLIENT_DATA_BUFFER:
                if (allocClientReadBufferState() > 0) {
                    // advance the Connection state and fall through
                    setOverallState(ConnectionStateEnum.READ_CLIENT_DATA);
                } else {
                    addToWorkQueue(false);
                    break;
                }

            case READ_CLIENT_DATA:
                readIntoDataBuffers();
                break;

            case CONN_FINISHED:
                System.out.println("WebServerConnState[" + connStateId + "] CONN_FINISHED");
                reset();

                // Now release this back to the free pool so it can be reused
                connectionStatePool.freeConnectionState(this);
                break;
        }
    }

    /*
     ** This function is used to determine the next state for the ConnectionState processing.
     **
     ** NOTE: This is only called from the start of the state machine.
     */
    private ConnectionStateEnum determineNextState() {

        /*
         ** Check if the header parsing is completed and if so, setup the initial reads
         **   for the content.
         **
         ** NOTE: This check is done prior to seeing if buffers need to be allocated to
         **   read in content data.
         */
        boolean headerParsed = httpHeaderParsed.get();
        if (headerParsed && !contentReadSetup) {
            /*
             ** Figure out how many buffers to read.
             */
            determineNextContentRead();
            contentReadSetup = true;
        }

        /*
         ** Are there outstanding buffers to be allocated. If the code had attempted to allocate
         **   buffers and failed, check if there is other work to do. No point trying the buffer
         **   allocation right away.
         **
         */
        boolean outOfMemory = bufferAllocationFailed.get();
        if (!outOfMemory) {
            if (requestedHttpBuffers > 0) {
                return ConnectionStateEnum.ALLOC_HTTP_BUFFER;
            }

            if (requestedDataBuffers > 0) {
                return ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER;
            }
        }

        /*
         ** Are there buffers waiting to have a read performed?
         */
        if (allocatedHttpBufferCnt > 0) {
            return ConnectionStateEnum.READ_HTTP_BUFFER;
        }

        /*
         ** The NIO.2 AsynchronousChannelRead can only have a single outstanding read at at time.
         **
         ** TODO: Support the NIO.2 read that can be passed in an array of ByteBuffers
         */
        if ((allocatedDataBuffers > 0)  && (outstandingDataReadCount.get() == 0)){
            return ConnectionStateEnum.READ_CLIENT_DATA;
        }

        /*
         ** Are there completed reads, priority is processing the HTTP header
         */
        int httpReadComp = httpBufferReadsCompleted.get();
        if (httpReadComp > 0) {
            return ConnectionStateEnum.PARSE_HTTP_BUFFER;
        }

        /*
         ** Check if the content has all been read in and then proceed to finishing the processing
         **
         ** TODO: Start adding in the steps to process the content data instead of just sending status
         */
        boolean doneReadingContent = contentAllRead.get();
        if (doneReadingContent) {
            if (!finalResponseSent) {
                return ConnectionStateEnum.SEND_XFR_DATA_RESP;
            }
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **    reads.
         */
        if (channelError.get()) {
            int httpReadsPending = outstandingHttpReadCount.get();
            int dataReadsPending = outstandingDataReadCount.get();

            if ((httpReadsPending == 0) && (dataReadsPending == 0)) {
                return ConnectionStateEnum.CONN_FINISHED;
            }
        }

        /*
         ** TODO: This is not really the exit point for the state machine, but until the
         **   steps for dealing with user data are added it is.
         */
        if (dataResponseSent.get() || finalResponseSendDone.get()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }

    /*
     ** Allocate a buffer to read HTTP header information into and associate it with this ConnectionState
     **
     ** The requestedHttpBuffers is not passed in since it is used to keep track of the number of buffers
     **   needed by this connection to perform another piece of work. The idea is that there may not be
     **   sufficient buffers available to allocate all that are requested, so there will be a wakeup call
     **   when buffers are available and then the connection will go back and try the allocation again.
     */
    private int allocHttpBufferState() {
        while (requestedHttpBuffers > 0) {
            BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_HTTP_FROM_CHAN, MemoryManager.SMALL_BUFFER_SIZE);
            if (bufferState != null) {
                allocatedHttpBufferQueue.add(bufferState);

                allocatedHttpBufferCnt++;
                requestedHttpBuffers--;
            } else {
                /*
                 ** Unable to allocate memory, come back later
                 */
                bufferAllocationFailed.set(true);
                break;
            }
        }

        return allocatedHttpBufferCnt;
    }

    /*
     ** This is used to start reads into one or more buffers. It looks for BufferState objects that have
     **   their state set to READ_FROM_CHAN. It then sends those buffers off to perform asynchronous reads.
     */
    private void readIntoMultipleBuffers() {
        BufferState bufferState;

        /*
         ** Only setup reads for allocated buffers
         */
        if (allocatedHttpBufferCnt > 0) {
            ListIterator<BufferState> iter = allocatedHttpBufferQueue.listIterator(0);
            while (iter.hasNext()) {
                bufferState = iter.next();
                iter.remove();

                allocatedHttpBufferCnt--;

                outstandingHttpReadCount.incrementAndGet();
                readFromChannel(bufferState);
            }
        }
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
    void httpReadCompleted(final BufferState bufferState) {
        int readCompletedCount;

        int readCount = outstandingHttpReadCount.decrementAndGet();
        try {
            httpReadDoneQueue.put(bufferState);
            readCompletedCount = httpBufferReadsCompleted.incrementAndGet();
        } catch (InterruptedException int_ex) {
            System.out.println("httpReadCompleted(" + connStateId + ") " + int_ex.getMessage());
            readCompletedCount = httpBufferReadsCompleted.get();
        }

        /*
         ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        System.out.println("WebServerConnState[" + connStateId + "].httpReadCompleted() HTTP readCount: " + readCount +
                " readCompletedCount: " + readCompletedCount);

        addToWorkQueue(false);
    }


    /*
     ** The following is used to handle the case when a read error occurred and the WebServerConnState needs
     **   to cleanup and release resources.
     **
     ** The call path to get here is:
     **    ServerWorkerThread chan.read() failed() callback
     **      --> BufferState.setReadState()
     **
     */
    @Override
    void readCompletedError(final BufferState bufferState) {
        int httpReadCount = 0;
        int dataReadCount = 0;

        channelError.set(true);

        if (bufferState.getBufferState() == BufferStateEnum.READ_WAIT_FOR_HTTP) {
            httpReadCount = outstandingHttpReadCount.decrementAndGet();
        } else {
            dataReadCount = outstandingDataReadCount.decrementAndGet();

            /*
             ** Need to increment the number of data buffer reads completed so that the clients will
             **   receive their callback with an error for the buffer.
             */
            try {
                dataReadDoneQueue.put(bufferState);
                dataBufferReadsCompleted.incrementAndGet();
            } catch (InterruptedException int_ex) {
                System.out.println("ERROR WebServerConnState[" + connStateId + "] readCompletedError() " + int_ex.getMessage());
            }
        }

        System.out.println("WebServerConnState[" + connStateId + "].readCompletedError() httpReadCount: " + httpReadCount +
                " dataReadCount: " + dataReadCount + " overallState: " + overallState);

        /*
         ** If there are outstanding reads in progress, need to wait for those to
         **   complete before cleaning up the ConnectionState
         */
        if ((httpReadCount == 0) && (dataReadCount == 0)){
            addToWorkQueue(false);
        }
    }

    /*
     ** This is called from the ServerWorkerThread chan.read() callback to update the state of the
     **   read buffer.
     **
     ** NOTE: This can only be called with the newState set to:
     **   BufferStateEnum.READ_DONE -> read completed without errors
     **   BufferStateEnum.READ_ERROR -> An error occurred while performing the read and the data in the buffer
     **     must be considered invalid.
     **
     ** TODO: Need to make the following thread safe when it modifies BufferState
     */
    void setReadState(final BufferState bufferState, final BufferStateEnum newState) {

        BufferStateEnum currBufferState = bufferState.getBufferState();

        bufferState.setReadState(newState);

        if (currBufferState == BufferStateEnum.READ_ERROR) {
            /*
             ** All the data has been read that will be read, but there is no point processing it as there
             **   was a channel error. Need to tell the ConnectionState to clean up and terminate this
             **   connection.
             */
            readCompletedError(bufferState);
        } else {
            if (currBufferState == BufferStateEnum.READ_WAIT_FOR_HTTP) {
                // Read of all the data is completed
                httpReadCompleted(bufferState);
            } else if (currBufferState == BufferStateEnum.READ_WAIT_FOR_DATA) {
                // Read of all the data is completed
                dataReadCompleted(bufferState);
            } else {
                System.out.println("ERROR: setReadState() invalid current state: " + bufferState.toString());
            }
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
        BufferState bufferState;
        ByteBuffer buffer;

        int bufferReadsDone = httpBufferReadsCompleted.get();
        if (bufferReadsDone > 0) {
            Iterator<BufferState> iter = httpReadDoneQueue.iterator();

            while (iter.hasNext()) {
                bufferState = iter.next();
                iter.remove();

                // TODO: Assert (bufferState.getState() == READ_HTTP_DONE)
                httpBufferReadsCompleted.decrementAndGet();

                buffer = bufferState.getBuffer();
                int bufferTextCapacity = buffer.position();
                buffer.rewind();
                buffer.limit(bufferTextCapacity);

                //displayBuffer(bufferState);
                ByteBuffer remainingBuffer;

                remainingBuffer = httpParser.parseHttpData(buffer, initialHttpBuffer);
                if (remainingBuffer != null) {
                    bufferState.swapByteBuffers(remainingBuffer);

                    int bytesRead = remainingBuffer.limit();
                    addDataBuffer(bufferState, bytesRead);
                } else {
                    /*
                     ** Set the BufferState to PARSE_DONE
                     ** TODO: When can the buffers used for the header be released?
                     */
                    bufferState.setHttpParseDone();
                }

                initialHttpBuffer = false;
            }

            /*
             ** Check if there needs to be another read to bring in more of the HTTP header
             */
            boolean headerParsed = httpHeaderParsed.get();
            if (!headerParsed) {
                /*
                 ** Allocate another buffer and read in more data
                 */
                requestedHttpBuffers++;
            }
        }
    }

    /*
     ** This is used to tell the connection management that the HTTP header has been parsed
     **   and the validation and authorization can take place.
     */
    void httpHeaderParseComplete(long contentLength) {
        System.out.println("WebServerConnState[" + connStateId + "] httpHeaderParseComplete() contentLength: " + contentLength + " curr: " + overallState);

        httpHeaderParsed.set(true);
        contentBytesToRead.set(contentLength);
    }

    /*
     ** This is used to setup the state machine and associated information to allow
     **   a write to take place on the passed in AsynchronousSocketChannel.
     */
    public void setChannel(final AsynchronousSocketChannel chan) {
        super.connChan = chan;
        overallState = ConnectionStateEnum.INITIAL_SETUP;

        /*
         ** Setup the WriteConnection at this point
         */
        writeConn = new WriteConnection(connStateId);

        writeConn.assignAsyncWriteChannel(chan);
    }

    /*
     ** Sets up the WriteConnection, but only does it once
     */
    private void setupWriteConnection() {
        if (writeConn == null) {
            writeConn = new WriteConnection(1);

            writeConn.assignAsyncWriteChannel(super.connChan);
        }
    }

    private WriteConnection getWriteConnection() {
        return writeConn;
    }


    /*
     ** This will send out a particular response type on the server channel
     */
    private void sendResponse() {

        // Allocate the Completion object specific to this operation
        setupWriteConnection();

        BufferState buffState = allocateResponseBuffer();
        if (buffState != null) {
            finalResponseSent = true;

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
    ** This allocates the buffer to send the response
     */
    private BufferState allocateResponseBuffer() {
        BufferState respBuffer = bufferStatePool.allocBufferState(this, BufferStateEnum.SEND_GET_DATA_RESPONSE, MemoryManager.MEDIUM_BUFFER_SIZE);

        responseBuffer = respBuffer;

        return respBuffer;
    }

    /*
     ** This is called when the status write completes back to the client.
     */
    public void statusWriteCompleted(final int bytesXfr, final ByteBuffer buffer) {

        BufferStateEnum currState;

        if (responseBuffer == null) {
            System.out.println("statusWriteCompleted: null responseBuffer" + connStateId);
        }

        currState = responseBuffer.getBufferState();
        System.out.println("statusWriteCompleted: " + connStateId + " " + currState.toString());

        switch (currState) {
            case SEND_GET_DATA_RESPONSE:
                dataResponseSent.set(true);
                break;

            case SEND_FINAL_RESPONSE:
                finalResponseSendDone.set(true);
                break;
        }

        bufferStatePool.freeBufferState(responseBuffer);
        responseBuffer = null;

        addToWorkQueue(false);
    }


    void releaseBufferState() {

        super.releaseBufferState();

        BufferState bufferState;
        ListIterator<BufferState> iter = allocatedHttpBufferQueue.listIterator(0);
        while (iter.hasNext()) {
            bufferState = iter.next();
            iter.remove();

            bufferStatePool.freeBufferState(bufferState);

            allocatedHttpBufferCnt--;
        }

    }

    void clearChannel() {
        super.clearChannel();
        httpHeaderParsed.set(false);

        super.connChan = null;
        writeConn = null;
    }

    void reset() {
        dataResponseSent.set(false);

        httpHeaderParsed.set(false);
        httpParser.resetHttpParser();

        /*
         ** Clear the write connection (it may already be null) since it will not be valid with the next
         **   use of this ConnectionState
         */
        writeConn = null;

        super.reset();
    }

}
