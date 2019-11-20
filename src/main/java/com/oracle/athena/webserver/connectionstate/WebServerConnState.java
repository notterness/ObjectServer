package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.StatusWriteCompletion;
import com.oracle.athena.webserver.server.WriteConnection;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
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
     */
    private BufferState responseBuffer;

    /*
    ** This is the class that determines the movement of the WebServerConnState through the
    **   pipelines that implement the actual requests. The first pipeline is always the
    **   HttpParsePipeline
     */
    private ConnectionPipelineMgr pipelineManager;
    private ContentReadPipelineMgr readPipelineMgr;
    private HttpParsePipelineMgr httpParsePipelineMgr;

    /*
    ** The following variables are used in the ContentReadPipeline class. This is used to determine the
    **   stages in the pipeline that is used to read in content data.
    **
    **   dataResponseSent - This is set to true when the HTTP response header has been sent.
    **   finalResponseSent -
    **   finalResponseSendDone - This is set to true when the callback from the socket write has taken place. This
    **     means that the ConnectionState can now be released back to the free pool.
     */
    private AtomicBoolean dataResponseSent;
    private boolean finalResponseSent;
    private AtomicBoolean finalResponseSendDone;


    /*
     ** The following variables are used to manage the read and parse HTTP headers pipeline.
     **
     ** The variables are used for the following:
     **    requestedHttpBuffers - This is how many buffers need to be allocated to read in the HTTP header. This
     **      will only be set to 0 or 1 for the current implementation.
     **    allocatedHttpBufferCount - This is how many buffers have been allocated and are waiting to have data
     **      read into them. As with requestedHttpBuffers, this can only be 0 or 1.
     **    outstandingHttpReadCount - This is the number of reads that are outstanding.
     **    httpBufferReadsCompleted - This is the number of buffers that have had data read into them and are ready
     **      to be run through the HTTP parser.
     **    httpHeaderParsed - This is set by a callback from the HTTP Parser when it determines that all the header
     **      data has been parsed and anything that follows will be content data.
     */
    private int requestedHttpBuffers;
    private int allocatedHttpBufferCount;

    private AtomicInteger outstandingHttpReadCount;
    private AtomicInteger httpBufferReadsCompleted;
    private AtomicBoolean httpHeaderParsed;


    /*
    ** The following is set to indicate that the first buffer is being sent through the HTTP parser.
    **   This is done so that some initial conditions can be validated before parsing begins.
     */
    private boolean initialHttpBuffer;

    /*
    ** The following are the queues used when work has been completed by one stage of the HTTP
    **   read and parse pipeline.
     */
    private LinkedList<BufferState> allocatedHttpBufferQueue;
    private BlockingQueue<BufferState> httpReadDoneQueue;


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
    ** The following is set when this connection is being used to send an out of resource response back to the
    **   client. This happens when the primary pool of connections has been depleted and the server cannot accept
    **   more connections until some complete.
     */
    private boolean outOfResourcesResponse;

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
        allocatedHttpBufferCount = 0;

        allocatedHttpBufferQueue = new LinkedList<>();

        httpBufferReadsCompleted = new AtomicInteger(0);
        initialHttpBuffer = true;
        httpReadDoneQueue = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);

        httpHeaderParsed = new AtomicBoolean(false);

        outOfResourcesResponse = false;

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

        httpParsePipelineMgr = new HttpParsePipelineMgr(this);
        readPipelineMgr = new ContentReadPipelineMgr(this);

        /*
        ** start with the http manager.
        */
        pipelineManager = httpParsePipelineMgr;
    }

    @Override
    public void stateMachine() {
        ConnectionStateEnum overallState = ConnectionStateEnum.INVALID_STATE;
        StateQueueResult result;

        result = pipelineManager.executePipeline();
        switch (result) {
            case STATE_RESULT_COMPLETE:
                // set next pipeline.
                addToWorkQueue(false);
                break;
            case STATE_RESULT_WAIT:
            case STATE_RESULT_REQUEUE:
                addToWorkQueue(false);
                return;
            case STATE_RESULT_FREE:
                overallState = ConnectionStateEnum.CONN_FINISHED;
                break;
        }

        switch (overallState) {

            case CHECK_SLOW_CHANNEL:
                if (timeoutChecker.inactivityThresholdReached()) {
                    /*
                     ** TOTDO: Need to close out the channel and this connection
                     */
                } else {
                    overallState = pipelineManager.nextPipelineStage();

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

            case READ_DONE:
                // TODO: Assert() if this state is ever reached
                break;

            case READ_DONE_ERROR:
                // Release all the outstanding buffer
                releaseBufferState();
                addToWorkQueue(false);
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
     ** The resets all the values for the HttpParsePipelineMgr
     */
    void resetHttpReadValues() {
        outstandingHttpReadCount.set(0);
        requestedHttpBuffers = 0;
        allocatedHttpBufferCount = 0;
        httpBufferReadsCompleted.set(0);
        httpHeaderParsed.set(false);
    }

    /*
    ** This returns if the Http headers have all been parsed. This is set via a callback
    **   from the HTTP parser.
     */
    boolean httpHeadersParsed() {
        return httpHeaderParsed.get();
    }

    /*
    ** Returns true if there is an outstanding request to allocate buffers to read in
    **   HTTP header data.
     */
    boolean httpBuffersNeeded() {
        return (requestedHttpBuffers > 0);
    }

    /*
    ** Returns true if buffers have been allocated to read in HTTP header data
    **   and the reads have not been started yet. This means the buffers are
    **   sitting on the allocatedHttpBufferQueue queue.
     */
    boolean httpBuffersAllocated() {
        return (allocatedHttpBufferCount > 0);
    }

    /*
    ** Returns true if there are buffers that have HTTP header data read into them
    **   and are waiting to be parsed. This means the buffers are sitting on the
    **   httpReadDoneQueue queue.
     */
    boolean httpBuffersReadyForParsing() {
        return (httpBufferReadsCompleted.get() > 0);
    }

    /*
    ** Returns the number of outstanding HTTP buffer reads there currently are.
     */
    int outstandingHttpBufferReads() {
        return outstandingHttpReadCount.get();
    }

    /*
     ** This is used to determine which pipeline to execute after the parsing and validation of the HTTP headers
     **   has been completed.
     */
    public void setupNextPipeline() {
        /*
        ** First check if this is an out of resources response
         */
        if (outOfResourcesResponse) {
            pipelineManager = new OutOfResourcePipelineMgr(this);
            return;
        }

        /*
         ** Now, based on the HTTP method, figure out the next pipeline
         */
        HttpMethodEnum method = casperHttpInfo.getMethod();
        switch (method) {
            case PUT_METHOD:
                pipelineManager = readPipelineMgr;
                break;

            case POST_METHOD:
                pipelineManager = readPipelineMgr;
                break;

            case INVALID_METHOD:
                break;
        }
    }

    /*
     ** Allocate a buffer to read HTTP header information into and associate it with this ConnectionState
     **
     ** The requestedHttpBuffers is not passed in since it is used to keep track of the number of buffers
     **   needed by this connection to perform another piece of work. The idea is that there may not be
     **   sufficient buffers available to allocate all that are requested, so there will be a wakeup call
     **   when buffers are available and then the connection will go back and try the allocation again.
     */
    public int allocHttpBufferState() {
        while (requestedHttpBuffers > 0) {
            BufferState bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_HTTP_FROM_CHAN, MemoryManager.SMALL_BUFFER_SIZE);
            if (bufferState != null) {
                allocatedHttpBufferQueue.add(bufferState);

                allocatedHttpBufferCount++;
                requestedHttpBuffers--;
            } else {
                /*
                 ** Unable to allocate memory, come back later
                 */
                bufferAllocationFailed.set(true);
                break;
            }
        }

        return allocatedHttpBufferCount;
    }


    /*
     ** This is used to start reads into one or more buffers. It looks for BufferState objects that have
     **   their state set to READ_FROM_CHAN. It then sends those buffers off to perform asynchronous reads.
     */
    public void readIntoMultipleBuffers() {
        BufferState bufferState;

        /*
         ** Only setup reads for allocated buffers
         */
        if (allocatedHttpBufferCount > 0) {
            ListIterator<BufferState> iter = allocatedHttpBufferQueue.listIterator(0);
            while (iter.hasNext()) {
                bufferState = iter.next();
                iter.remove();

                allocatedHttpBufferCount--;

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
    private void httpReadCompleted(final BufferState bufferState) {
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
    public void readCompletedError(final BufferState bufferState) {
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
                " dataReadCount: " + dataReadCount);

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
    public void setReadState(final BufferState bufferState, final BufferStateEnum newState) {

        if (newState == BufferStateEnum.READ_ERROR) {
            /*
             ** All the data has been read that will be read, but there is no point processing it as there
             **   was a channel error. Need to tell the ConnectionState to clean up and terminate this
             **   connection.
             */
            readCompletedError(bufferState);
        } else {
            BufferStateEnum currBufferState = bufferState.getBufferState();

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

        bufferState.setReadState(newState);
    }


    /*
     ** This function walks through all the buffers that have reads completed and pushes
     **   then through the HTTP Parser.
     ** Once the header buffers have been parsed, they can be released. The goal is to not
     **   recycle the data buffers, so those may not need to be sent through the HTTP Parser'
     **   and instead should be handled directly.
     */
    public void parseHttp() {
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
        System.out.println("WebServerConnState[" + connStateId + "] httpHeaderParseComplete() contentLength: " + contentLength);

        httpHeaderParsed.set(true);
        contentBytesToRead.set(contentLength);
    }

    /*
     ** This is used to setup the state machine and associated information to allow
     **   a write to take place on the passed in AsynchronousSocketChannel.
     */
    public void setChannel(final AsynchronousSocketChannel chan) {
        super.setAsyncChannel(chan);

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
    public void sendResponse(final int resultCode) {

        // Allocate the Completion object specific to this operation
        setupWriteConnection();

        BufferState buffState = allocateResponseBuffer();
        if (buffState != null) {
            finalResponseSent = true;

            ByteBuffer respBuffer = resultBuilder.buildResponse(buffState, resultCode, true);

            int bytesToWrite = respBuffer.position();
            respBuffer.flip();

            WriteConnection writeConn = getWriteConnection();
            StatusWriteCompletion statusComp = new StatusWriteCompletion(this, writeConn, respBuffer,
                    getConnStateId(), bytesToWrite, 0);
            writeThread.writeData(writeConn, statusComp);

            HttpStatus.Code result = HttpStatus.getCode(resultCode);
            if (result != null) {
                System.out.println("sendResponse[" + connStateId + "] resultCode: " + result.getCode() + " " + result.getMessage());
            }
        } else {
            System.out.println("sendResponse[" + connStateId + "] unable to allocate response buffer");
        }
    }


    /*
    ** This allocates the buffer to send the response
     */
    private BufferState allocateResponseBuffer() {
        BufferState respBuffer = bufferStatePool.allocBufferState(this, BufferStateEnum.SEND_FINAL_RESPONSE, MemoryManager.MEDIUM_BUFFER_SIZE);

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
        System.out.println("statusWriteCompleted[" + connStateId + "] " + currState.toString());

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

    /*
    ** Returns if the data response has been sent. This is the intermediate response
    **   asking the client to send the content data
     */
    boolean hasDataResponseBeenSent() {
        return dataResponseSent.get();
    }

    /*
    ** Returns if the final response (either good or bad) has been sent to the
    **   client. This means it has been queued up to be written on the wire,
    **   but does not mean it has actually been sent.
     */
    boolean hasFinalResponseBeenSent() {
        return finalResponseSent;
    }

    /*
    ** Returns true when the write of the final response to the client has been
    **   successfully written on the wire.
     */
    boolean finalResponseSent() {
        return finalResponseSendDone.get();
    }

    void resetResponses() {
        dataResponseSent.set(false);
        finalResponseSent = false;
        finalResponseSendDone.set(false);
    }

    public void releaseBufferState() {

        super.releaseBufferState();

        BufferState bufferState;
        ListIterator<BufferState> iter = allocatedHttpBufferQueue.listIterator(0);
        while (iter.hasNext()) {
            bufferState = iter.next();
            iter.remove();

            bufferStatePool.freeBufferState(bufferState);

            allocatedHttpBufferCount--;
        }

    }

    public void clearChannel() {
        super.clearChannel();

        writeConn = null;
    }

    /*
    ** This is called just after the connection is allocated to tell that is will be used to send back an
    **   error to the client indicating that there are insufficient resources currently.
     */
    public void setOutOfResourceResponse() {
        System.out.println("WebServerConnState[" + connStateId + "] setOutOfResourceResponse()");

        outOfResourcesResponse = true;
    }


    public void reset() {
        dataResponseSent.set(false);

        /*
        ** Setup the HTTP parser for a new ByteBuffer stream
         */
        casperHttpInfo = null;
        casperHttpInfo = new CasperHttpInfo(this);

        initialHttpBuffer = true;
        //httpParser.resetHttpParser();
        httpParser = null;
        httpParser = new ByteBufferHttpParser(casperHttpInfo);


        /*
         ** Clear the write connection (it may already be null) since it will not be valid with the next
         **   use of this ConnectionState
         */
        writeConn = null;

        outOfResourcesResponse = false;

        /*
        ** Reset the pipeline manager back to default
         */
        pipelineManager = httpParsePipelineMgr;

        super.reset();
    }

    public int getRequestedHttpBuffers() {
        return requestedHttpBuffers;
    }

    public void setRequestedHttpBuffers(int requestedHttpBuffers) {
        this.requestedHttpBuffers = requestedHttpBuffers;
    }
}
