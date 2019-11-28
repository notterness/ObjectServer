package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.StatusWriteCompletion;
import com.oracle.athena.webserver.server.WriteConnection;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.server.WebServerAuths;
import org.eclipse.jetty.http.HttpStatus;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebServerConnState extends ConnectionState {

    private static final Logger LOG = LoggerFactory.getLogger(WebServerConnState.class);

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
    private OutOfResourcePipelineMgr outOfResourcePipelineMgr;
    private SSLHandshakePipelineMgr sslHandshakePipelineMgr;

    /*
    ** The following variables are used in the ContentReadPipeline class. This is used to determine the
    **   stages in the pipeline that is used to read in content data.
    **
    **   finalResponseSent - This is set in the sendResponse() method when the BufferState has been allocated
    **     to write the response to the write channel. It is set prior to the write taking place.
    **
    **   dataRequestResponseSendDone - This is set to true when the HTTP response header has been sent. This is used when the
    **     intermediate response telling the client to send the content data has been sent,
    **
    **   finalResponseSendDone - This is set to true when the callback from the socket write has taken place and the
    **     processResponseWriteDone() method has been called from the state machine. This means that the
    **     ConnectionState can now be released back to the free pool.
    *
    **   responseChannelWriteDone - This is set when the callback from the channel write completes
     */
    private boolean dataRequestResponseSendDone;
    private boolean finalResponseSent;
    private AtomicBoolean responseChannelWriteDone;
    private boolean finalResponseSendDone;


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
    private AtomicInteger httpBufferReadsUnwrapNeeded;
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
        finalResponseSent = false;
        dataRequestResponseSendDone = false;
        finalResponseSendDone = false;

        responseChannelWriteDone = new AtomicBoolean(false);

        outstandingHttpReadCount = new AtomicInteger(0);
        httpBufferReadsUnwrapNeeded = new AtomicInteger(0);
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


    public void setupSSL() {
        setupInitial();
        setSSLHandshakeRequired(false);
        setSSLBuffersNeeded(true);
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
        sslHandshakePipelineMgr = new SSLHandshakePipelineMgr(this);
        readPipelineMgr = new ContentReadPipelineMgr(this);
        outOfResourcePipelineMgr = new OutOfResourcePipelineMgr(this);
    }

    public void selectPipelineManagers() {
        pipelineManager = (sslContext == null) ? httpParsePipelineMgr : sslHandshakePipelineMgr;
    }

    /*
     ** This is what actually performs the work
     */
    @Override
    public void stateMachine() {
        StateQueueResult result;

        result = pipelineManager.executePipeline();
        switch (result) {
            case STATE_RESULT_COMPLETE:
                // set next pipeline.
                addToWorkQueue(false);
                break;
            case STATE_RESULT_WAIT:
                addToWorkQueue(true);
                break;

            case STATE_RESULT_REQUEUE:
                addToWorkQueue(false);
                break;
            case STATE_RESULT_FREE:
                break;
        }
    }


    /*
    ** This is used to help debug stuck connection problems.
     */
    @Override
    public ConnectionStateEnum getNextState(){
        LOG.error("WebServerConnState[" + connStateId + "] connOnDelayedQueue: " + connOnDelayedQueue +
                " connOnExecutionQueue: " + connOnExecutionQueue);
        LOG.error("WebServerConnState[" + connStateId + "] responseChannelWriteDone: " + responseChannelWriteDone.get() +
                " finalResponseSendDone: " + finalResponseSendDone);


        workerThread.dumpWorkerThreadQueues();
        return pipelineManager.nextPipelineStage();
    }

    /*
     ** This checks if there is a slow client
     */
    boolean checkSlowClientChannel() {
        boolean continueExecution = true;

        if (timeoutChecker.inactivityThresholdReached()) {
            /*
             ** TODO: Channel is closed and now the test needs to be updated to validate the
             **   behavior.
             */
            LOG.info("WebServerConnState[" + connStateId + "] connection timeout");
            closeChannel();
        } else {
            ConnectionStateEnum overallState = pipelineManager.nextPipelineStage();

            /*
             ** Need to wait for something to kick the state machine to a new state
             **
             ** The ConnectionState will get put back on the execution queue when an external
             **   operation completes.
             */
            if (overallState == ConnectionStateEnum.CHECK_SLOW_CHANNEL) {
                continueExecution = false;
            }
        }

        return continueExecution;
    }

    /*
    **
    */
    @Override
    public void setupInitial() {
        super.setupInitial();

        LOG.info("WebServerConnState[" + connStateId + "] INITIAL_SETUP server");
        requestedHttpBuffers = 1;
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

    boolean httpBuffersReadyToUnwrap() {
        return (httpBufferReadsUnwrapNeeded.get() > 0);
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
    void setupNextPipeline() {
        /*
        ** First check if this is an out of resources response
         */
        if (outOfResourcesResponse) {
            LOG.info("WebServerConnState[" + connStateId + "] setupNextPipeline() outOfResourcePipelineMgr");

            pipelineManager = outOfResourcePipelineMgr;
            return;
        }

        /*
         ** Now, based on the HTTP method, figure out the next pipeline
         */
        HttpMethodEnum method = casperHttpInfo.getMethod();
        LOG.info("WebServerConnState[" + connStateId + "] setupNextPipeline() " + method.toString());

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

    public void setupNextSSLPipeline() {
        pipelineManager = httpParsePipelineMgr;
    }

    /*
     ** Allocate a buffer to read HTTP header information into and associate it with this ConnectionState
     **
     ** The requestedHttpBuffers is not passed in since it is used to keep track of the number of buffers
     **   needed by this connection to perform another piece of work. The idea is that there may not be
     **   sufficient buffers available to allocate all that are requested, so there will be a wakeup call
     **   when buffers are available and then the connection will go back and try the allocation again.
     */
    int allocHttpBufferState() {
        BufferState bufferState;

        while (requestedHttpBuffers > 0) {
            if (isSSL()) {
                int appBufferSize = engine.getSession().getApplicationBufferSize();
                int netBufferSize = engine.getSession().getPacketBufferSize();
                bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_HTTP_FROM_CHAN,
                                                                appBufferSize, netBufferSize);
            } else {
                bufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_HTTP_FROM_CHAN, MemoryManager.SMALL_BUFFER_SIZE);
            }

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
    void readIntoMultipleBuffers() {
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
                LOG.info("buffer removed from alloc " + bufferState);
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

            readCompletedCount = isSSL() ? httpBufferReadsUnwrapNeeded.incrementAndGet() :
                                           httpBufferReadsCompleted.incrementAndGet();
        } catch (InterruptedException int_ex) {
            LOG.info("httpReadCompleted(" + connStateId + ") " + int_ex.getMessage());
            readCompletedCount = isSSL() ? httpBufferReadsUnwrapNeeded.get() : httpBufferReadsCompleted.get();
        }

        /*
         ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        LOG.info("WebServerConnState[" + connStateId + "] httpReadCompleted() HTTP readCount: " + readCount +
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
        if (!readErrorQueue.offer(bufferState)) {
            LOG.info("ERROR WebServerConnState[" + connStateId + "] readCompletedError() offer failed");
        }

        addToWorkQueue(false);
    }

    /*
    ** The following is used to synchronize the work for processing the read errors with the primary worker thread.
    **   If this is not done, there is a race condition between when the channelError() gets set and the pipeline
    **   manager potentially deciding the connection can be closed out.
    **
    ** This will return true if all the outstanding reads have been completed and further checking can take place.
    *
    ** TODO: The buffers can be released here. That is what differentiates this from the smae method in
    **   ClientConnState
     */
    boolean processReadErrorQueue() {
        int httpReadCount = 0;
        int dataReadCount = 0;

        if (!readErrorQueue.isEmpty()) {
            BufferState bufferState;
            Iterator<BufferState> iter = readErrorQueue.iterator();
            while (iter.hasNext()) {
                bufferState = iter.next();
                LOG.info("process read error " + bufferState.getBufferState() + " " +bufferState );
                iter.remove();

                if (bufferState.getBufferState() == BufferStateEnum.READ_WAIT_FOR_HTTP) {
                    httpReadCount = outstandingHttpReadCount.decrementAndGet();
                } else {
                    dataReadCount = outstandingDataReadCount.decrementAndGet();
                }

                /*
                ** Release the buffer back to the free pool
                 */
                bufferStatePool.freeBufferState(bufferState);
            }
        }

        LOG.info("WebServerConnState[" + connStateId + "] processReadError() httpReadCount: " + httpReadCount +
                " dataReadCount: " + dataReadCount);

        /*
        ** Mark that there was a channel read error so that the pipeline manager can clean up the
        **   connection and close it out.
         */
        channelError = true;

        /*
         ** If there are outstanding reads in progress, need to wait for those to
         **   complete before cleaning up the ConnectionState
         */
        if ((httpReadCount == 0) && (dataReadCount == 0)){
            return true;
        }

        return false;
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

            /*
             ** Only do this for good reads at this point.
             */
            bufferState.setReadState(newState);

            if (currBufferState == BufferStateEnum.READ_WAIT_FOR_HTTP) {
                // Read of all the data is completed
                httpReadCompleted(bufferState);
            } else if (currBufferState == BufferStateEnum.READ_WAIT_FOR_DATA) {
                // Read of all the data is completed
                dataReadCompleted(bufferState);
            } else {
                LOG.info("ERROR: setReadState() invalid current state: " + bufferState.toString());
            }
        }
    }

    public void sslReadUnwrap() {
        ByteBuffer clientAppData;
        ByteBuffer clientNetData;

        int bufferReadsDone = httpBufferReadsUnwrapNeeded.get();
        if (bufferReadsDone > 0) {
            for (BufferState bufferState : httpReadDoneQueue) {

                clientAppData = bufferState.getBuffer();
                clientNetData = bufferState.getNetBuffer();
                clientNetData.flip();
                while (clientNetData.hasRemaining()) {
                    clientAppData.clear();
                    SSLEngineResult result;
                    try {
                        result = engine.unwrap(clientNetData, clientAppData);
                    } catch (SSLException e) {
                        System.out.println("Unable to unwrap data.  Will be evident in parse.");
                        e.printStackTrace();
                        return;
                    }
                    switch (result.getStatus()) {
                        case OK:
                            System.out.println("Unwrapped data!");
                            httpBufferReadsUnwrapNeeded.decrementAndGet();
                            httpBufferReadsCompleted.incrementAndGet();
                            break;
                        case BUFFER_OVERFLOW:
                            //    clientAppData = enlargeApplicationBuffer(engine, peerAppData);
                            break;
                        case BUFFER_UNDERFLOW:
                            // no data was read or the TLS packet was incomplete.
                            break;
                        case CLOSED:
                            //("Client wants to close connection...");
                            //closeConnection(socketChannel, engine);
                            return;
                        default:
                            //Log this state that can't happen
                            System.out.println("Illegal state: " + result.getStatus().toString());
                            break;
                    }
                }
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
    void parseHttp() {
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
                buffer.flip();

                //displayBuffer(bufferState);
                ByteBuffer remainingBuffer;

                remainingBuffer = httpParser.parseHttpData(buffer, initialHttpBuffer);
                if (remainingBuffer != null) {
                    /*
                    ** Allocate a new BufferState to hold the remaining data
                     */
                    BufferState newBufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_DONE, MemoryManager.SMALL_BUFFER_SIZE);
                    bufferState.copyByteBuffer(remainingBuffer);

                    int bytesRead = remainingBuffer.limit();
                    addDataBuffer(newBufferState, bytesRead);
                }

                /*
                ** Set the BufferState to PARSE_DONE. Setting the BufferState is sort of redundant as
                **   the buffer is going to be released to the free pool immediately after. It might be
                **   useful at some point for validation code.
                **
                ** NOTE: The assumption is that once the HTTP request data buffer has been parsed, there
                **   is no reason to keep it around.
                 */
                bufferState.setBufferHttpParseDone();
                bufferStatePool.freeBufferState(bufferState);

                initialHttpBuffer = false;
            }

            /*
             ** Check if there needs to be another read to bring in more of the HTTP header
             */
            boolean headerParsed = httpHeaderParsed.get();
            if (!headerParsed) {
                /*
                 ** Allocate another buffer and read in more data. But, do not
                 **   allocate if there was a parsing error.
                 */
                if (!httpParsingError.get()) {
                    requestedHttpBuffers++;
                } else {
                    LOG.info("WebServerConnState[" + connStateId + "] parsing error, no allocation");
                }
            } else {
                LOG.info("WebServerConnState[" + connStateId + "] header was parsed");
            }
        }
    }

    /*
     ** This is used to tell the connection management that the HTTP header has been parsed
     **   and the validation and authorization can take place.
     */
    void httpHeaderParseComplete(long contentLength) {
        LOG.info("WebServerConnState[" + connStateId + "] httpHeaderParseComplete() contentLength: " + contentLength);

        httpHeaderParsed.set(true);

        contentBytesToRead.set(contentLength);
    }

    /*
    ** This is used to perform the Embargo checking for this request
     */
    void checkEmbargo(WebServerAuths auths) {
        final String namespace = casperHttpInfo.getNamespace();
        final String bucket = casperHttpInfo.getBucket();
        final String object = casperHttpInfo.getObject();

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.V2)
                .setOperation(CasperOperation.PUT_OBJECT)
                .setNamespace(namespace)
                .setBucket(bucket)
                .setObject(object)
                .build();
        auths.getEmbargoV3().enter(embargoV3Operation);
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

    @Override
    public void setSslContext(SSLContext sslContext) {
        super.initSslEngine(sslContext);
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
     ** This will send out a specified response type on the server channel back to the client
     */
    void sendResponse(final int resultCode) {

        // Allocate the Completion object specific to this operation
        setupWriteConnection();

        BufferState buffState = allocateResponseBuffer();
        if (buffState != null) {
            finalResponseSent = true;

            ByteBuffer respBuffer = resultBuilder.buildResponse(buffState, resultCode, true);

            if (isSSL()) {
                try {
                    sslWrapData(buffState);
                } catch (IOException e) {
                    //FIXME: Any SSLEngine problems, drop connection, give up
                    LOG.info("WebServerConnState[" + connStateId + "] SSLEngine threw " + e.toString());
                    e.printStackTrace();
                }
                respBuffer = buffState.getNetBuffer();
            }

            int bytesToWrite = respBuffer.position();
            respBuffer.flip();
            LOG.info("responseBuf alloc " + respBuffer);

            WriteConnection writeConn = getWriteConnection();
            StatusWriteCompletion statusComp = new StatusWriteCompletion(this, writeConn, respBuffer,
                    getConnStateId(), bytesToWrite, 0);
            writeThread.writeData(writeConn, statusComp);

            HttpStatus.Code result = HttpStatus.getCode(resultCode);
            if (result != null) {
                LOG.info("WebServerConnState[" + connStateId + "] sendResponse() resultCode: " + result.getCode() + " " + result.getMessage());
            } else {
                LOG.info("WebServerConnState[" + connStateId + "] sendResponse() resultCode: " + result.getCode());
            }
        } else {
            /*
            ** If we are out of memory to allocate a response, might as well close out the connection and give up.
             */
            LOG.info("WebServerConnState[" + connStateId + "] sendResponse() unable to allocate response buffer");

            /*
            ** Set the finalResponseSendDone flag to allow the state machine to complete.
            **
            ** TODO: Most likely if the final response cannot be sent, we may need to mark an error for this connection
            **   and cleanup any items related to this connection. These may include writes to the Storage Server and
            **   other things like that.
             */
            finalResponseSendDone = true;
        }
    }


    /*
    ** This allocates the buffer to send the response
     */
    private BufferState allocateResponseBuffer() {
        BufferState respBuffer;
        if (!isSSL()) {
            respBuffer = bufferStatePool.allocBufferState(this,
                    BufferStateEnum.SEND_FINAL_RESPONSE, MemoryManager.MEDIUM_BUFFER_SIZE);
        } else {
            int appBufferSize = engine.getSession().getApplicationBufferSize();
            int netBufferSize = engine.getSession().getPacketBufferSize();

            respBuffer = bufferStatePool.allocBufferState(this,
                    BufferStateEnum.SEND_FINAL_RESPONSE, appBufferSize, netBufferSize);
        }
        responseBuffer = respBuffer;

        return respBuffer;
    }

    /*
     ** This is called when the status write completes back to the client.
     **
     ** TODO: Pass the buffer back instead of relying on the responseBuffer
     */
    public void statusWriteCompleted(final ByteBuffer buffer) {
        responseChannelWriteDone.set(true);
        addToWorkQueue(false);
    }

    /*
    ** This is triggered after the write callback has taken place
     */
    void processResponseWriteDone() {
        if (!responseChannelWriteDone.get()) {
            LOG.warn("WebServerConnState[" + connStateId + "] processResponseWriteDone() should not be here");
        }

        if (responseBuffer == null) {
            LOG.warn("WebServerConnState[" + connStateId + "] processResponseWriteDone: null responseBuffer");
            return;
        }

        BufferStateEnum currState = responseBuffer.getBufferState();
        LOG.info("WebServerConnState[" + connStateId + "] processResponseWriteDone() currState: " + currState.toString());

        switch (currState) {
            case SEND_GET_DATA_RESPONSE:
                dataRequestResponseSendDone = true;
                break;
            case SEND_FINAL_RESPONSE:
                finalResponseSendDone = true;
                break;
        }

        LOG.info("processResponse " + responseBuffer);
        bufferStatePool.freeBufferState(responseBuffer);
        responseBuffer = null;

        /*
        ** Need to clear the flag so that the state machine doesn't sit running though this state time after
        **   time.
         */
        responseChannelWriteDone.set(false);
     }

    /*
    **
     */
    boolean getResponseChannelWriteDone() {
        boolean responseWriteCompleted = responseChannelWriteDone.get();
        //LOG.info("WebServerConnState[" + connStateId + "] getResponseChannelWriteDone() " +
        //        responseWriteCompleted);

        return responseWriteCompleted;
    }


    /*
    ** Returns if the data response has been sent. This is the intermediate response
    **   asking the client to send the content data
     */
    boolean hasDataResponseBeenSent() {
        return dataRequestResponseSendDone;
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
        return finalResponseSendDone;
    }

    void resetResponses() {
        dataRequestResponseSendDone = false;
        finalResponseSent = false;
        finalResponseSendDone = false;
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

            LOG.info("WebServerConnState[" + connStateId + "] releaseBufferState() " + allocatedHttpBufferCount);
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
        LOG.info("WebServerConnState[" + connStateId + "] setOutOfResourceResponse()");

        outOfResourcesResponse = true;
    }

    /*
     ** Accessor function related to the HTTP Parser and when an error occurs.
     **
     ** getHttpParserError() will return HttpStatus.OK_200 if there is no error, otherwise it will return
     **   the value set to indicate the parsing error.
     */
    @Override
    public int getHttpParseStatus() {
        int parsingStatus = HttpStatus.OK_200;
        if (httpParsingError.get()) {
            parsingStatus = casperHttpInfo.getParseFailureCode();
        }

        return parsingStatus;
    }

    /*
    ** This is a placeholder for the encryption step. Currently it is used to release the content buffers
     */
    void releaseContentBuffers() {
        BufferState[] md5DoneBuffers = new BufferState[0];
        md5DoneBuffers = dataMd5DoneQueue.toArray(md5DoneBuffers);
        BufferState[] readDoneBuffers = new BufferState[0];
        readDoneBuffers = dataReadDoneQueue.toArray(readDoneBuffers);

        LOG.info("allocatedBuffers " + allocatedDataBufferQueue.size());
        LOG.info("md5 " + dataMd5DoneQueue.size() + " data " + dataReadDoneQueue.size() );
        LOG.info("readError " + readErrorQueue.size());
        for (BufferState bufferState : md5DoneBuffers) {
            bufferStatePool.freeBufferState(bufferState);
            dataMd5DoneQueue.remove(bufferState);

            int digestCompleted = dataBufferDigestCompleted.decrementAndGet();
            LOG.info("release Md5 " + bufferState);
            LOG.info("WebServerConnState[" + connStateId + "] releaseContentBuffers() " + digestCompleted);
        }

        for (BufferState bufferState : readDoneBuffers) {
            bufferStatePool.freeBufferState(bufferState);
            dataReadDoneQueue.remove(bufferState);

            int readsCompleted = dataBufferReadsCompleted.decrementAndGet();
            LOG.info("release bufferReads " + bufferState);
            LOG.info("WebServerConnState[" + connStateId + "] releaseContentBuffers() " + readsCompleted);
        }
    }

    /*
     ** This is the final cleanup of the connection before it is put back in the free pool. It is expected
     **   that when the connection is pulled from the free pool it is in a pristine state and can be used
     **   to handle a new connection.
     */
    public void reset() {
        dataRequestResponseSendDone = false;

        /*
        ** Setup the HTTP parser for a new ByteBuffer stream
         */
        casperHttpInfo = null;
        casperHttpInfo = new CasperHttpInfo(this);

        initialHttpBuffer = true;

        // TODO: Why does resetHttpParser() not do what is expected (meaning leaving it in a state to start parsing a new stream)?
        //httpParser.resetHttpParser();
        httpParser = null;
        httpParser = new ByteBufferHttpParser(casperHttpInfo);

        resetHttpReadValues();
        resetContentAllRead();
        resetResponses();

        responseChannelWriteDone.set(false);

        /*
        ** Reset the pipeline back to the Parse HTTP Pipeline for the next iteration of this connection
         */
        pipelineManager = httpParsePipelineMgr;

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

        /*
        ** Now release this back to the free pool so it can be reused
         */
        connectionStatePool.freeConnectionState(this);
    }
}
