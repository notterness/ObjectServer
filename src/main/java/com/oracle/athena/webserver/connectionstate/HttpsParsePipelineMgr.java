package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.eclipse.jetty.http.HttpStatus;

import java.util.function.Function;

class HttpsParsePipelineMgr extends ConnectionPipelineMgr {

    private final WebServerSSLConnState connectionState;

    private boolean initialStage;

    private Function<WebServerConnState, StateQueueResult> httpsParseInitialSetup = wsConn -> {
        wsConn.setupInitial();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseAllocHttpBuffer = wsConn -> {
        if (wsConn.allocHttpBuffer() == 0){
            return StateQueueResult.STATE_RESULT_WAIT;
        } else {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseReadHttpBuffer = wsConn -> {
        wsConn.readIntoHttpBuffers();

        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseUnwrapHttpsBuffer = wsConn -> {
        wsConn.unwrapHttp();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseHttpBuffer = wsConn -> {
        wsConn.parseHttpBuffers();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseConnFinished = wsConn -> {
        initialStage = true;

        wsConn.reset();

        return StateQueueResult.STATE_RESULT_FREE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseCheckSlowConnection = wsConn -> {
        if (wsConn.checkSlowClientChannel()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        } else {
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseSendXferResponse = wsConn -> {
        wsConn.sendResponse(wsConn.getHttpParseStatus());
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsCleanupFreeHttpsBuffers = wsConn -> {
        if (wsConn.cleanupFreeHttpsBuffers()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        } else {
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseSetupNextPipeline = wsConn -> {
        initialStage = true;
        wsConn.resetHttpReadValues();
        wsConn.resetContentAllRead();
        wsConn.setupNextPipeline();
        return StateQueueResult.STATE_RESULT_COMPLETE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseProcessReadError = wsConn -> {
        if (wsConn.processReadErrorQueue()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> httpsParseProcessFinalResponseSend = wsConn -> {
        wsConn.processResponseWriteDone();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    public HttpsParsePipelineMgr(WebServerSSLConnState connectionState) {
        super(connectionState, new StateMachine<>());
        this.connectionState = connectionState;

        initialStage = true;

        /*
        ** Reset the state of the pipeline
         */
        this.connectionState.resetHttpReadValues();

        /*
        ** This must be set to false here as there may be content data included in a buffer used to
        **   to read in the HTTP headers.
         */
        this.connectionState.resetContentAllRead();

        /*
        ** In error cases, this pipeline will send out error responses.
         */
        this.connectionState.resetResponses();
        connectionStateMachine.addStateEntry(ConnectionStateEnum.INITIAL_SETUP, new StateEntry<>(httpsParseInitialSetup));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry(httpsParseCheckSlowConnection));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_HTTP_BUFFER, new StateEntry(httpsParseAllocHttpBuffer));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.READ_HTTP_BUFFER, new StateEntry(httpsParseReadHttpBuffer));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.UNWRAP_HTTP_BUFFER, new StateEntry(httpsParseUnwrapHttpsBuffer));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PARSE_HTTP_BUFFER, new StateEntry(httpsParseHttpBuffer));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry(httpsParseConnFinished));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry(httpsParseSetupNextPipeline));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SEND_FINAL_RESPONSE, new StateEntry(httpsParseSendXferResponse));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_READ_ERROR, new StateEntry(httpsParseProcessReadError));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND, new StateEntry(httpsParseProcessFinalResponseSend));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.FREE_HTTP_BUFFERS, new StateEntry(httpsCleanupFreeHttpsBuffers));

   }

    /*
    ** This determines the pipeline stages used to read in and parse the HTTP headers.
     */
    @Override
    public ConnectionStateEnum nextPipelineStage() {
        /*
         ** Are there outstanding buffers to be allocated. If the code had attempted to allocate
         **   buffers and failed, check if there is other work to do. No point trying the buffer
         **   allocation right away.
         **
         */
        if (!connectionState.outOfMemory() && connectionState.httpBuffersNeeded()) {
            return ConnectionStateEnum.ALLOC_HTTP_BUFFER;
        }

        /*
         ** Are there completed reads ready to unwrap
         */
        if (connectionState.httpBuffersReadyToUnwrap()) {
            return ConnectionStateEnum.UNWRAP_HTTP_BUFFER;
        }

        /*
         ** Are there buffers waiting to have a read performed?
         */
        if (connectionState.httpBuffersAllocated()) {
            return ConnectionStateEnum.READ_HTTP_BUFFER;
        }

        /*
         ** Are there completed reads, priority is processing the HTTP header
         */
        if (connectionState.httpBuffersReadyForParsing()) {
            return ConnectionStateEnum.PARSE_HTTP_BUFFER;
        }

        /*
        ** Check if there are buffers in error that need to be processed.
         */
        if (connectionState.readErrorQueueNotEmpty()) {
            return ConnectionStateEnum.PROCESS_READ_ERROR;
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **    reads.
         */
        if (connectionState.hasChannelFailed()) {
            if (connectionState.outstandingHttpBufferReads() == 0) {
                return ConnectionStateEnum.CONN_FINISHED;
            }

            // TODO: Need to log something to indicate waiting for reads to complete
            return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
        }

        if (connectionState.getResponseChannelWriteDone()) {
            return ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND;
        }

        /*
         ** The status has been sent so cleanup the connection. This check needs to proceed any checks
         **   that will cause a response to be sent.
         */
        if (connectionState.finalResponseSent()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        /*
         ** Check if there has been an HTTP headers parsing error and if so, return the appropriate
         **    response to the client.
         */
        if (connectionState.getHttpParseStatus() != HttpStatus.OK_200) {
            return ConnectionStateEnum.SEND_FINAL_RESPONSE;
        }

        if (connectionState.httpHeadersParsed() && connectionState.isHttpReadDoneBuffers()) {
            return ConnectionStateEnum.FREE_HTTP_BUFFERS;
        }

        /*
         ** Check if the header parsing is completed and if so, setup the initial reads
         **   for the content.
         **
         ** NOTE: This check is done prior to seeing if buffers need to be allocated to
         **   read in content data.
         */
        if (connectionState.httpHeadersParsed()) {
            /*
             ** Figure out how many buffers to read.
             */
            return ConnectionStateEnum.SETUP_NEXT_PIPELINE;
        }


        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }
}
