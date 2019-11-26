package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.eclipse.jetty.http.HttpStatus;

import java.util.function.Function;

class HttpsParsePipelineMgr extends ConnectionPipelineMgr {

    private WebServerSSLConnState connectionState;

    private boolean initialStage;

    private StateMachine httpsParseStateMachine;


    private Function httpsParseInitialSetup = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn) {
            wsConn.setupInitial();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpsParseAllocHttpBuffer = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn) {
            if (wsConn.allocHttpBufferState() == 0){
                return StateQueueResult.STATE_RESULT_WAIT;
            }
            else {
                return StateQueueResult.STATE_RESULT_CONTINUE;
            }
        }
    };

    private Function httpsParseReadHttpBuffer = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn) {
            wsConn.readIntoMultipleBuffers();


            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
    };

    private Function httpsParseUnwrapHttpsBuffer = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn) {
            wsConn.unwrap();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpsParseHttpBuffer = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn) {
            wsConn.parseHttp();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpsParseConnFinished = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn) {
            initialStage = true;

            wsConn.reset();

            return StateQueueResult.STATE_RESULT_FREE;
        }
    };

    private Function httpsParseCheckSlowConnection = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn) {
            if (wsConn.checkSlowClientChannel()) {
                return StateQueueResult.STATE_RESULT_CONTINUE;
            } else {
                return StateQueueResult.STATE_RESULT_WAIT;
            }
        }
    };

    private Function httpsParseSendXferResponse = new Function<WebServerSSLConnState, StateQueueResult>() {
        public StateQueueResult apply(WebServerSSLConnState wsConn) {
            wsConn.sendResponse(wsConn.getHttpParseStatus());
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    private Function httpsParseSetupNextPipeline = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn){
            initialStage = true;
            wsConn.resetHttpReadValues();
            wsConn.resetContentAllRead();
            wsConn.setupNextPipeline();
            return StateQueueResult.STATE_RESULT_COMPLETE;
        }
    };

    private Function httpsParseProcessReadError = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn){
            if (wsConn.processReadErrorQueue()) {
                return StateQueueResult.STATE_RESULT_CONTINUE;
            }
            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
    };

    private Function httpsParseProcessFinalResponseSend = new Function<WebServerSSLConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerSSLConnState wsConn){
            wsConn.processResponseWriteDone();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    public HttpsParsePipelineMgr(WebServerSSLConnState connState) {

        super(connState);

        connectionState = connState;

        initialStage = true;

        /*
        ** Reset the state of the pipeline
         */
        connectionState.resetHttpReadValues();

        /*
        ** This must be set to false here as there may be content data included in a buffer used to
        **   to read in the HTTP headers.
         */
        connectionState.resetContentAllRead();

        /*
        ** In error cases, this pipeline will send out error responses.
         */
        connectionState.resetResponses();
        httpsParseStateMachine = new StateMachine();
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.INITIAL_SETUP, new StateEntry(httpsParseAllocHttpBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry(httpsParseCheckSlowConnection));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_HTTP_BUFFER, new StateEntry(httpsParseAllocHttpBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.READ_HTTP_BUFFER, new StateEntry(httpsParseReadHttpBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.UNWRAP_HTTP_BUFFER, new StateEntry(httpsParseUnwrapHttpsBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.PARSE_HTTP_BUFFER, new StateEntry(httpsParseHttpBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry(httpsParseConnFinished));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry(httpsParseSetupNextPipeline));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.SEND_FINAL_RESPONSE, new StateEntry(httpsParseSendXferResponse));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_READ_ERROR, new StateEntry(httpsParseProcessReadError));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND, new StateEntry(httpsParseProcessFinalResponseSend));

   }

    /*
    ** This determines the pipeline stages used to read in and parse the HTTP headers.
     */
    public ConnectionStateEnum nextPipelineStage() {

        /*
        ** Perform the initial setup
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.INITIAL_SETUP;
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
         ** Are there buffers waiting to have a read performed?
         */
        if (connectionState.httpBuffersAllocated()) {
            return ConnectionStateEnum.READ_HTTP_BUFFER;
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

        /*
         ** Are there completed reads ready to unwrap
         */
        if (connectionState.httpBuffersReadyToUnwrap()) {
            return ConnectionStateEnum.UNWRAP_HTTP_BUFFER;
        }

        /*
         ** Are there completed reads, priority is processing the HTTP header
         */
        if (connectionState.httpBuffersReadyForParsing()) {
            return ConnectionStateEnum.PARSE_HTTP_BUFFER;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }

    public StateQueueResult executePipeline() {
        StateQueueResult result;
        ConnectionStateEnum nextVerb;

        do {
            nextVerb = nextPipelineStage();
            result = httpsParseStateMachine.stateMachineExecute(connectionState, nextVerb);

        } while (result == StateQueueResult.STATE_RESULT_CONTINUE);

        return result;
    }
}
