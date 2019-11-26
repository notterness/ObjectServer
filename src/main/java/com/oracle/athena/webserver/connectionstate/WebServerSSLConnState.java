package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.SSLEngineMgr;
import com.oracle.athena.webserver.server.StatusWriteCompletion;
import com.oracle.athena.webserver.server.WriteConnection;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class WebServerSSLConnState extends WebServerConnState {

    private static final Logger LOG = LoggerFactory.getLogger(WebServerSSLConnState.class);
    public static final int NUM_SSL_APP_BUFFERS = 2;
    public static final int NUM_SSL_NET_BUFFERS = 2;

    private enum SSLConnState {
        SSL_CONN_STATE_INIITAL,
        SSL_CONN_STATE_HANDSHAKE,
        SSL_CONN_STATE_HTTPS,
        SSL_CONN_STATE_RX_OBJECT;
    }

    private SSLConnState sslConnState;
    private SSLEngineMgr sslEngineMgr;
    private SSLHandshakePipelineMgr sslHandshakePipelineMgr;
    protected HttpsParsePipelineMgr httpsParsePipelineMgr;
    private SSLContentReadPipelineMgr sslReadReadPipelineMgr;

    private AtomicInteger httpBufferReadsUnwrapNeeded;


    /*
     ** The following is used to release this ConnectionState back to the free pool.
     */
    private ConnectionStatePool<WebServerSSLConnState> connectionStatePool;


    public WebServerSSLConnState(final ConnectionStatePool<WebServerSSLConnState> connectionStatePool,
                                 SSLEngineMgr sslEngineMgr, final int uniqueId) {
        super(uniqueId);
        this.connectionStatePool = connectionStatePool;
        this.sslEngineMgr = sslEngineMgr;
        httpBufferReadsUnwrapNeeded = new AtomicInteger(0);
    }

    @Override
    public void setupInitial() {
        super.setupInitial();
        setSSLHandshakeRequired(false);
        setSSLBuffersNeeded(true);
    }

    @Override
    public void start() {
        super.start();

        sslHandshakePipelineMgr = new SSLHandshakePipelineMgr(this);
        httpsParsePipelineMgr = new HttpsParsePipelineMgr(this);
        sslReadReadPipelineMgr = new SSLContentReadPipelineMgr(this);
        sslConnState = SSLConnState.SSL_CONN_STATE_INIITAL;
        setupNextPipeline();
    }

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
            bufferAllocationFailed.set(true);
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

        if (status == SSLEngineMgr.Status.FINISHED) {
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
            LOG.info("WebServerConnState[" + connStateId + "] setupNextPipeline() outOfResourcePipelineMgr");

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
                LOG.info("WebServerConnState[" + connStateId + "] setupNextPipeline() " + method.toString());

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
        }
    }

    /*
     ** Allocate buffers to read SSL encoded HTTP header information into and associate it with this ConnectionState
     **
     ** The requestedHttpBuffers is not passed in since it is used to keep track of the number of buffers
     **   needed by this connection to perform another piece of work. The idea is that there may not be
     **   sufficient buffers available to allocate all that are requested, so there will be a wakeup call
     **   when buffers are available and then the connection will go back and try the allocation again.
     */
    @Override
    int allocHttpBufferState() {
        while (requestedHttpBuffers > 0) {
            BufferState bufferState = bufferStatePool.allocBufferState(this,
                    BufferStateEnum.READ_HTTP_FROM_CHAN,
                    sslEngineMgr.getAppBufferSize(),
                    sslEngineMgr.getNetBufferSize());

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

    boolean httpBuffersReadyToUnwrap() {
        return (httpBufferReadsUnwrapNeeded.get() > 0);
    }

    public void unwrap() {
        SSLEngineResult result;
        int bufferReadsDone = httpBufferReadsUnwrapNeeded.get();
        if (bufferReadsDone > 0) {
            for (BufferState bufferState : httpReadDoneQueue) {
                result = sslEngineMgr.unwrap(bufferState);
                if (result == null) {
                    //TODO: if not able to unwrap
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


    /*
        Mark that the read is done and ready for unwrap.  On the NIO completion thread.
     */
    @Override
    void httpReadCompleted(final BufferState bufferState) {
        int readCompletedCount;

        int readCount = outstandingHttpReadCount.decrementAndGet();
        try {
            httpReadDoneQueue.put(bufferState);
            readCompletedCount = httpBufferReadsUnwrapNeeded.incrementAndGet();
        } catch (InterruptedException int_ex) {
            LOG.info("httpReadCompleted(" + connStateId + ") " + int_ex.getMessage());
            readCompletedCount = httpBufferReadsUnwrapNeeded.get();
        }

        /*
         ** Update the channel's health timeout
         */
        timeoutChecker.updateTime();

        LOG.info("WebServerSLLConnState[" + connStateId + "] httpReadCompleted() HTTP readCount: " + readCount +
                 " readCompletedCount: " + readCompletedCount);

        addToWorkQueue(false);
    }

    /*
     ** This will send out a specified response type on the server channel back to the client
     */
    @Override
    void sendResponse(final int resultCode) {

        // Allocate the Completion object specific to this operation
        setupWriteConnection();

        BufferState buffState = sslEngineMgr.allocBufferState(this, bufferStatePool,
                                    BufferStateEnum.SEND_FINAL_RESPONSE);
        if (buffState != null) {
            responseBuffer = buffState;
            finalResponseSent = true;

            resultBuilder.buildResponse(buffState, resultCode, true);

            try {
                sslEngineMgr.wrap(buffState);
            } catch (IOException e) {
                //FIXME: Any SSLEngine problems, drop connection, give up
                LOG.info("WebServerConnState[" + connStateId + "] SSLEngine threw " + e.toString());
                e.printStackTrace();
            }

            ByteBuffer respBuffer = buffState.getNetBuffer();

            int bytesToWrite = respBuffer.position();
            respBuffer.flip();

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
         ** Reset the pipeline back to the handshake
         */
        pipelineManager = sslHandshakePipelineMgr;

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

}