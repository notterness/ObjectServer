package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.HttpMethodEnum;
import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.BlockingPipelineThreadPool;
import com.oracle.athena.webserver.server.StatusWriteCompletion;
import com.oracle.athena.webserver.server.WriteConnection;
import com.oracle.pic.casper.webserver.server.WebServerAuths;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class WebServerSSLConnState extends WebServerConnState {

    private static final Logger LOG = LoggerFactory.getLogger(WebServerSSLConnState.class);
    public static final int NUM_SSL_APP_BUFFERS = 2;
    public static final int NUM_SSL_NET_BUFFERS = 2;

    private enum SSLConnState {
        SSL_CONN_STATE_INIITAL,
        SSL_CONN_STATE_HANDSHAKE,
        SSL_CONN_STATE_HTTPS,
        SSL_CONN_STATE_RX_OBJECT,
        SSL_CONN_STATE_CLOSE
    }

    private SSLConnState sslConnState;
    private SSLContext sslContext;
    private SSLEngineMgr sslEngineMgr;
    private SSLHandshakePipelineMgr sslHandshakePipelineMgr;
    protected HttpsParsePipelineMgr httpsParsePipelineMgr;
    private SSLContentReadPipelineMgr sslReadReadPipelineMgr;
    private boolean sslHandshakeRequired;
    private boolean sslHandshakeSuccess;
    private boolean sslBuffersNeeded;

    private AtomicInteger httpBufferReadsUnwrapNeeded;

    private BlockingQueue<BufferState> dataReadDoneUnwrap;

    /*
     ** The following is used to release this ConnectionState back to the free pool.
     */
    private ConnectionStatePool<WebServerSSLConnState> connectionStatePool;


    public WebServerSSLConnState(final WebServerFlavor flavor, final WebServerAuths auths,
                                 final ConnectionStatePool<WebServerSSLConnState> connectionStatePool,
                                 final BlockingPipelineThreadPool blockingPipelineThreadPool,
                                 SSLContext sslContext, final int uniqueId) {
        super(flavor, auths,null, blockingPipelineThreadPool, uniqueId);
        this.connectionStatePool = connectionStatePool;
        this.sslContext = sslContext;
        httpBufferReadsUnwrapNeeded = new AtomicInteger(0);
        dataReadDoneUnwrap = new LinkedBlockingQueue<>(MAX_OUTSTANDING_BUFFERS * 2);
    }

    @Override
    public void setupInitial() {
        super.setupInitial();
        setSSLHandshakeRequired(false);
        setSSLHandshakeSuccess(false);
        setSSLBuffersNeeded(true);
    }

    public void setupReadContent() {
        setSSLHandshakeRequired(false);
        setSSLHandshakeSuccess(false);
        setSSLBuffersNeeded(false);
    }

    @Override
    public void start() {
        super.start();

        sslHandshakePipelineMgr = new SSLHandshakePipelineMgr(this);
        httpsParsePipelineMgr = new HttpsParsePipelineMgr(this);
        sslReadReadPipelineMgr = new SSLContentReadPipelineMgr(this);
        initSSLEngineMgr();

        sslConnState = SSLConnState.SSL_CONN_STATE_INIITAL;
        setupNextPipeline();
    }

    private void initSSLEngineMgr() {
        sslEngineMgr = new SSLEngineMgr(sslContext);
        sslEngineMgr.setUseClientMode(false);
    }

    @Override
    public boolean isSSL() {return true;}

    /*
     ** Allocate a buffers for SSL handshaking.
     ** Server and client buffers are supposed to be large enough to hold all message data the server
     ** will send and expects to receive from the client. Since the messages to be exchanged will usually be less
     ** than 16KB long the capacity of these fields should also be smaller.  Expected buffer sizes are retrieved
     ** from the session object.
     */
    public int allocSSLHandshakeBuffers() {
        int numBuffers = sslEngineMgr.allocSSLHandshakeBuffers(this, bufferStatePool);
        if (numBuffers < (NUM_SSL_APP_BUFFERS + NUM_SSL_NET_BUFFERS)) {
            bufferAllocationFailed.set(true);
        } else {
            bufferAllocationFailed.set(false);
            setSSLBuffersNeeded(false);
        }

        return numBuffers;
    }

    public void freeSSLHandshakeBuffers() {
        sslEngineMgr.freeSSLHandshakeBuffers(bufferStatePool);
    }


    public void beginHandshake() throws SSLException {
        sslEngineMgr.beginHandshake();
        setSSLHandshakeRequired(true);
        setSSLHandshakeSuccess(false);
    }

    public void doSSLHandshake() {
        SSLEngineMgr.Status status = sslEngineMgr.doSSLHandshake(this);

        if (status == SSLEngineMgr.Status.FINISHED ||
            status == SSLEngineMgr.Status.NOT_HANDSHAKING) {
            setSSLHandshakeRequired(false);
            setSSLHandshakeSuccess(true);
        }
    }

    /*
     ** This is used to determine which pipeline to execute after the parsing and validation of the HTTP headers
     **   has been completed.
     */
    @Override
    void setupNextPipeline() {

        /*
         ** First check if this is an out of resources response
         */
        if (outOfResourcesResponse) {
            LOG.info("WebServerSSLConnState[" + connStateId + "] setupNextPipeline() outOfResourcePipelineMgr");

            pipelineManager = outOfResourcePipelineMgr;
            return;
        }

        switch (sslConnState) {
            case SSL_CONN_STATE_INIITAL:
                sslConnState = SSLConnState.SSL_CONN_STATE_HANDSHAKE;
                pipelineManager = sslHandshakePipelineMgr;
                break;

            case SSL_CONN_STATE_HANDSHAKE:
                sslConnState = SSLConnState.SSL_CONN_STATE_HTTPS;
                pipelineManager = httpsParsePipelineMgr;
                break;

            case SSL_CONN_STATE_HTTPS: {
                /*
                 ** Now, based on the HTTP method, figure out the next pipeline
                 */
                HttpMethodEnum method = casperHttpInfo.getMethod();
                LOG.info("WebServerSSLConnState[" + connStateId + "] setupNextPipeline() " + method.toString());

                switch (method) {
                    case PUT_METHOD:
                        sslConnState = SSLConnState.SSL_CONN_STATE_RX_OBJECT;
                        pipelineManager = sslReadReadPipelineMgr;
                        break;

                    case POST_METHOD:
                        sslConnState = SSLConnState.SSL_CONN_STATE_RX_OBJECT;
                        pipelineManager = sslReadReadPipelineMgr;
                        break;

                    case INVALID_METHOD:
                        break;
                }
                break;
            }
			
            case SSL_CONN_STATE_RX_OBJECT:
                sslConnState = SSLConnState.SSL_CONN_STATE_CLOSE;
                pipelineManager = sslHandshakePipelineMgr;
                break;

            case SSL_CONN_STATE_CLOSE:
                break;
        }

    }

    @Override
    protected int contentBufferSize() {
        return sslEngineMgr.getNetBufferSize();
    }

    @Override
    BufferState allocBufferState(BufferStateEnum state) {
        return sslEngineMgr.allocBufferState(this, bufferStatePool, state);
    }

    /*
     ** Allocate a buffer to read HTTP header information into and associate it with this ConnectionState
     **
     ** The requestedHttpBuffers is not passed in since it is used to keep track of the number of buffers
     **   needed by this connection to perform another piece of work. The idea is that there may not be
     **   sufficient buffers available to allocate all that are requested, so there will be a wakeup call
     **   when buffers are available and then the connection will go back and try the allocation again.
     */
    @Override
    int allocHttpBuffer() {

        while (requestedHttpBuffers > 0) {
            BufferState bufferState = allocBufferState(BufferStateEnum.READ_HTTP_FROM_CHAN);

            if (bufferState != null) {
                /* Check if any used buffers exist */
                Iterator<BufferState> iter = httpReadDoneQueue.iterator();

                if (iter.hasNext()) {
                    BufferState usedBufferState = iter.next();
                    iter.remove();
                    ByteBuffer usedNetBuffer = usedBufferState.getNetBuffer();
                    int netLimit = bufferState.getNetBuffer().limit();

                    /* check for underflow condition */
                    if (usedNetBuffer.hasRemaining()) {
                        bufferState.copyNetBuffer(usedNetBuffer);
                        /* adjust pointers */
                        ByteBuffer newNetBuffer = bufferState.getNetBuffer();
                        newNetBuffer.position(newNetBuffer.limit());
                        newNetBuffer.limit(netLimit);
                    }
                    bufferStatePool.freeBufferState(usedBufferState);
                }

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

    /* Returns TRUE if successfully cleaned up, FALSE if a buffer still needs
       allocation
     */
    boolean cleanupFreeHttpsBuffers() {
        Iterator<BufferState> iter = httpReadDoneQueue.iterator();

        if (iter.hasNext()) {
            BufferState usedBufferState = iter.next();
            ByteBuffer usedNetBuffer = usedBufferState.getNetBuffer();

            /* check for underflow condition */
            if (usedNetBuffer.hasRemaining()) {
                /* Allocate a data buffer */
                BufferState dataBufferState = allocateContentDataBuffers();
                if (dataBufferState != null) {

                    /* this should be in common allocator */
                    int bufferSize = dataBufferState.getBuffer().limit();
                    contentBytesAllocated.addAndGet(bufferSize);


                    ByteBuffer dataNetBuffer = dataBufferState.getNetBuffer();
                    int netLimit = dataNetBuffer.limit();

                    /* copy remaining contents of net buffer into data buffer */
                    dataBufferState.copyNetBuffer(usedNetBuffer);

                    /* adjust pointers */
                    dataNetBuffer.position(dataNetBuffer.limit());
                    dataNetBuffer.limit(netLimit);

                    allocatedDataBuffers += dataBufferState.count();
                    allocatedDataBufferQueue.add(dataBufferState);

                    LOG.info("WebServerSLLConnState[" + connStateId + "] allocContentReadBuffers(2) allocatedDataBuffers: " + allocatedDataBuffers);

                    iter.remove();
                    bufferStatePool.freeBufferState(usedBufferState);
                    return true;
                }
                /* This routine will be retried by the pipeline until the httpReadDoneQueue is cleared */
            } else {
                //No unused packet data.  Remove from queue and free.
                iter.remove();
                bufferStatePool.freeBufferState(usedBufferState);
                return true;
            }
        }
        return false;
    }

    /*
     ** This is used to start reads into one or more buffers. It looks for BufferState objects that have
     **   their state set to READ_FROM_CHAN. It then sends those buffers off to perform asynchronous reads.
     */
    void readIntoHttpBuffers() {
        BufferState bufferState;

        /*
         ** Only setup reads for allocated buffers
         */
        if (allocatedHttpBufferCount > 0) {
            ListIterator<BufferState> iter = allocatedHttpBufferQueue.listIterator(0);
            if (iter.hasNext()) {
                bufferState = iter.next();
                iter.remove();

                allocatedHttpBufferCount--;

                outstandingHttpReadCount++;
                readFromChannel(bufferState);
            }
        }
    }

    boolean httpBuffersReadyToUnwrap() {
        return (httpBufferReadsUnwrapNeeded.get() > 0);
    }

    public void unwrapHttp() {
        SSLEngineResult result;
        int bufferReadsDone = httpBufferReadsUnwrapNeeded.get();
        if (bufferReadsDone > 0) {
            for (BufferState bufferState : httpReadDoneQueue) {

                ByteBuffer netBuffer = bufferState.getNetBuffer();
                int bytesRead = netBuffer.position();
                int netRemaining = netBuffer.remaining();
                int netLimit = netBuffer.limit();

                outstandingHttpReadCount--;

                bufferState.getBuffer().clear();
                bufferState.getNetBuffer().flip();
                result = sslEngineMgr.unwrap(bufferState);
                if (result == null) {
                    //TODO: if not able to unwrap
                }

                switch (result.getStatus()) {
                    case OK:
                        httpBufferReadsUnwrapNeeded.decrementAndGet();
                        httpBufferReadsCompleted.incrementAndGet();
                        break;
                    case BUFFER_OVERFLOW:
                        break;
                    case BUFFER_UNDERFLOW:
                        /* if have not received a full packet, go back and get more */
                        if (netRemaining > 0) {
                            //Get more data to complete the TLS packet
                            outstandingDataReadCount.incrementAndGet();
                            bufferState.setBufferState(BufferStateEnum.READ_HTTP_FROM_CHAN);
                            //restore original limit
                            httpBufferReadsUnwrapNeeded.decrementAndGet();
                            netBuffer.limit(netLimit);
                            readFromChannel(bufferState);
                        } else {
                            //This case should have content.
                            assert bufferState.getBuffer().position() > 0;
                            //First go ahead and parse what we have
                            httpBufferReadsUnwrapNeeded.decrementAndGet();
                            httpBufferReadsCompleted.incrementAndGet();
                        }
                        break;
                    case CLOSED:
                        //("Client wants to close connection...");
                        //TODO: handle
                        return;
                    default:
                        //Log this state that shouldn't happen
                        System.out.println("Unknown state: " + result.getStatus().toString());
                        break;
                }
            }

        }
    }

    public void unwrapData() {
        SSLEngineResult result;
        BufferState bufferState;
        Iterator<BufferState> iter = dataReadDoneUnwrap.iterator();

        while (iter.hasNext()) {
            bufferState = iter.next();

            // check for buffer overflow processing
            if (bufferState.getBuffer() == null) {
                // get actual buffer size
                int bufferSize = MemoryManager.allocatedBufferCapacity(sslEngineMgr.getAppBufferSize());
                BufferState newBufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_DONE, bufferSize + 1);
                if (newBufferState != null){
                    bufferState.assignBuffer(newBufferState.getBuffer(), BufferStateEnum.READ_DONE);
                } else {
                    // try again later
                    return;
                }
            }
            ByteBuffer netBuffer = bufferState.getNetBuffer();
            ByteBuffer appBuffer = bufferState.getBuffer();
            int netRemaining = 0;
            int netLimit = netBuffer.limit();

            //Check for underflow processing
            if (bufferState.getBufferState() != BufferStateEnum.UNWRAP_UNDERFLOW) {
                netRemaining = netBuffer.remaining();
                appBuffer.clear();
                netBuffer.flip();
            }

            result = sslEngineMgr.unwrap(bufferState);
            if (result == null) {
                //TODO: if not able to unwrap
            }

            switch (result.getStatus()) {
                case OK:
                    iter.remove();
                    outstandingDataReadCount.decrementAndGet();

                    addDataBuffer(bufferState, appBuffer.position());
                    break;
                case BUFFER_OVERFLOW:
                    //TODO: free app buffer, attempt to allocate another.
                    int bufferSize = MemoryManager.allocatedBufferCapacity(sslEngineMgr.getAppBufferSize());
                    BufferState ovBufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_DONE, bufferSize + 1);
                    if (ovBufferState != null){
                        bufferStatePool.freeBuffer(bufferState);
                        bufferState.assignBuffer(ovBufferState.getBuffer(), BufferStateEnum.READ_DONE);
                    }
                    break;
                case BUFFER_UNDERFLOW:
                    /* if have not received a full packet, go back and get more */
                    bufferState.setBufferState(BufferStateEnum.UNWRAP_UNDERFLOW);
                    if (netRemaining > 0) {
                        //Remove buffer from dataReadDoneUnwrap
                        iter.remove();

                        //Get more data to complete the TLS packet
                        //restore original limit
                        netBuffer.limit(netLimit);
                        //read from channel
                        bufferState.setBufferState(BufferStateEnum.READ_DATA_FROM_CHAN);
                        readFromChannel(bufferState);
                    } else {
                        /* Received full buffer, but have an incomplete TLS packet.
                           copy remaining to next buffer.
                         */
                        ListIterator<BufferState> queueIter = allocatedDataBufferQueue.listIterator(0);
                        if (queueIter.hasNext()) {
                            BufferState nextBufferState = queueIter.next();
                            ByteBuffer nextNetBuffer = nextBufferState.getNetBuffer();

                            //Transfer remaining packet bytes to next netbuffer
                            nextBufferState.copyNetBuffer(bufferState.getNetBuffer());

                            //Adjust position and limit
                            nextNetBuffer.position(nextNetBuffer.limit());
                            /** TODO: adjust limit of all allocated buffers to get
                             ** optimal packet size to prevent underflow conditions.
                             */
                            nextNetBuffer.limit(netLimit);

                            //This case should have content.
                            assert bufferState.getBuffer().position() > 0;
                            //Add this buffer to data buffer queue
                            iter.remove();
                            outstandingDataReadCount.decrementAndGet();
                            addDataBuffer(bufferState, appBuffer.position());
                        } else {
                            /*
                                Need to allocate a buffer.  After the buffer is allocated, the unwrap
                                case will be repeated with this bufferState still on the unwrap queue.
                                and this underflow case will be repeated with buffer on the allocated
                                queue.
                             */
                            addRequestedDataBuffer();
                        }
                    }
                    break;
                case CLOSED:
                    //TODO
                    return;
                default:
                    //Log this state that shouldn't happen
                    System.out.println("Unknown state: " + result.getStatus().toString());
                    break;
            }
        }

    }

    @Override
    public BufferState allocateContentDataBuffers() {
        return sslEngineMgr.allocBufferState(this, bufferStatePool, BufferStateEnum.READ_DATA_FROM_CHAN);
    }

    /*
        Mark that the read is done and ready for unwrap.  On the NIO completion thread.
     */
    @Override
    void httpReadCompleted(final BufferState bufferState) {
        int readCompletedCount;

        try {
            httpReadDoneQueue.put(bufferState);
            readCompletedCount = httpBufferReadsUnwrapNeeded.incrementAndGet();
        } catch (InterruptedException int_ex) {
            LOG.info("WebServerSSLConnState: httpReadCompleted(" + connStateId + ") " + int_ex.getMessage());
            readCompletedCount = httpBufferReadsUnwrapNeeded.get();
        }

        /*
         ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        LOG.info("WebServerSLLConnState[" + connStateId + "] httpReadCompleted() HTTP readCompletedCount: " + readCompletedCount);

        addToWorkQueue(false);
    }

    /*
     ** This function walks through all the buffers that have reads completed and pushes
     **   then through the HTTP Parser.
     ** Once the header buffers have been parsed, they can be released. The goal is to not
     **   recycle the data buffers, so those may not need to be sent through the HTTP Parser'
     **   and instead should be handled directly.
     */
    @Override
    void parseHttpBuffers() {
        BufferState bufferState;
        ByteBuffer buffer;
        Iterator<BufferState> iter;

        int bufferReadsDone = httpBufferReadsCompleted.get();
        if (bufferReadsDone > 0) {
            iter = httpReadDoneQueue.iterator();

            if (iter.hasNext()) {
                bufferState = iter.next();

                outstandingHttpReadCount--;

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
                    BufferState newBufferState = bufferStatePool.allocBufferState(this, BufferStateEnum.READ_DONE, remainingBuffer.limit());
                    newBufferState.copyByteBuffer(remainingBuffer);

                    int bytesRead = remainingBuffer.limit();
                    contentBytesAllocated.addAndGet(bytesRead);
                    addDataBuffer(newBufferState, bytesRead);
                }

                /*
                 ** Set the BufferState to PARSE_DONE.
                 */
                bufferState.setBufferHttpParseDone();

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
                    LOG.info("WebServerSSLConnState[" + connStateId + "] parsing error, no allocation");
                }
            } else {
                LOG.info("WebServerSSLConnState[" + connStateId + "] header was parsed");
            }
        }
    }

    boolean isHttpReadDoneBuffers() { return (httpReadDoneQueue.size() > 0); }

    /*
        Mark that the read is done and ready for unwrap.  On the NIO completion thread.
     */
    @Override
    public void dataReadCompleted(final BufferState bufferState) {

        try {
            dataReadDoneUnwrap.put(bufferState);
        } catch (InterruptedException int_ex) {
            /*
             ** TODO: This is an error case and the connection needs to be closed
             */
            LOG.info("WebServerSSLConnState: dataReadCompleted(" + connStateId + ") " + int_ex.getMessage());
        }

        /*
         ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        LOG.info("WebServerSSLConnState[" + connStateId + "] dataReadCompleted(), unwrap ready");

    }

    /*
     ** This will send out a specified response type on the server channel back to the client
     */
    @Override
    void sendResponse(final int resultCode) {

        // Allocate the Completion object specific to this operation
        setupWriteConnection();

        BufferState buffState = allocBufferState(BufferStateEnum.SEND_FINAL_RESPONSE);
        if (buffState != null) {
            responseBuffer = buffState;
            finalResponseSent.set(true);

            HttpStatus.Code result = HttpStatus.getCode(resultCode);
            if (result != null) {
                LOG.info("WebServerSSLConnState[" + connStateId + "] sendResponse() resultCode: " + result.getCode() + " " + result.getMessage());
            } else {
                LOG.info("WebServerSSLConnState[" + connStateId + "] sendResponse() resultCode: " + result.getCode());
            }
        } else {
            /*
             ** If we are out of memory to allocate a response, might as well close out the connection and give up.
             */
            LOG.info("WebServerSSLConnState[" + connStateId + "] sendResponse() unable to allocate response buffer");

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
     ** This is called when the status write completes back to the client.
     **
     ** TODO: Pass the buffer back instead of relying on the responseBuffer
     */
    @Override
    public void statusWriteCompleted(final ByteBuffer buffer) {
        responseChannelWriteDone.set(true);
        LOG.info("WebServerConnState[" + connStateId + "] statusWriteCompleted");
    }

    /*
     ** This is the final cleanup of the connection before it is put back in the free pool. It is expected
     **   that when the connection is pulled from the free pool it is in a pristine state and can be used
     **   to handle a new connection.
     */
    @Override
    public void reset() {
        //super.reset();

        dataRequestResponseSendDone = false;

        /*
         ** Setup the HTTP parser for a new ByteBuffer stream
         */
        casperHttpInfo = null;
        //casperHttpInfo = new CasperHttpInfo(this);

        initialHttpBuffer = true;

        // TODO: Why does resetHttpParser() not do what is expected (meaning leaving it in a state to start parsing a new stream)?
        //httpParser.resetHttpParser();
        httpParser = null;

        resetHttpReadValues();
        resetContentAllRead();
        resetResponses();
        freeSSLHandshakeBuffers();

        responseChannelWriteDone.set(false);

        /*
         ** Reset the pipeline back to the handshake
         */
        pipelineManager = sslHandshakePipelineMgr;
        sslConnState = SSLConnState.SSL_CONN_STATE_HANDSHAKE;
        initSSLEngineMgr();

        /*
         ** Clear the write connection (it may already be null) since it will not be valid with the next
         **   use of this ConnectionState
         */
        writeConn = null;

        outOfResourcesResponse = false;

        /*
         ** Now release this back to the free pool so it can be reused
         */
        connectionStatePool.freeConnectionState(this);
    }

    public boolean isSSLHandshakeRequired() {
        return sslHandshakeRequired;
    }

    public void setSSLHandshakeRequired(boolean sslHandshakeRequired) {
        this.sslHandshakeRequired = sslHandshakeRequired;
    }

    public boolean isSSLHandshakeSuccess() {
        return sslHandshakeSuccess;
    }

    public void setSSLHandshakeSuccess(boolean sslHandshakeSuccess) {
        this.sslHandshakeSuccess = sslHandshakeSuccess;
    }

    public boolean isSSLBuffersNeeded() {
        return sslBuffersNeeded;
    }

    public void setSSLBuffersNeeded( boolean sslBuffersNeeded ){
        this.sslBuffersNeeded = sslBuffersNeeded;
    }

    public int getDataBuffersUnwrapRequired() { return dataReadDoneUnwrap.size(); }

    void startSSLClose() {
        channelError = false;
        finalResponseSendDone = false;
        setSSLHandshakeRequired(true);
        setSSLHandshakeSuccess(false);
        setSSLBuffersNeeded(true);

        sslEngineMgr.sslClose(this);
    }
}