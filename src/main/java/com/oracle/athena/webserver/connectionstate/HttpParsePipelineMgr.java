package com.oracle.athena.webserver.connectionstate;

import org.eclipse.jetty.http.HttpStatus;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;

import java.util.function.Function;

class HttpParsePipelineMgr extends ConnectionPipelineMgr {

    private WebServerConnState connectionState;

    private boolean initialStage;

    private StateMachine httpParseStateMachine;


    private Function httpParseInitialSetup = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.setupInitial();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpParseAllocHttpBuffer = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            if (wsConn.allocHttpBufferState() == 0){
                return StateQueueResult.STATE_RESULT_WAIT;
            }
            else {
                return StateQueueResult.STATE_RESULT_CONTINUE;
            }
        }
    };

    private Function httpParseReadHttpBuffer = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.readIntoMultipleBuffers();
            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
    };

    private Function httpParseHttpBuffer = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.parseHttp();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpParseConnFinished = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            initialStage = true;

            wsConn.reset();

            return StateQueueResult.STATE_RESULT_FREE;
        }
    };

    private Function httpParseCheckSlowConnection = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            if (wsConn.checkSlowClientChannel()) {
                return StateQueueResult.STATE_RESULT_REQUEUE;
            } else {
                return StateQueueResult.STATE_RESULT_WAIT;
            }
        }
    };

    private Function httpParseSendXferResponse = new Function<WebServerConnState, StateQueueResult>() {
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.sendResponse(wsConn.getHttpParseStatus());
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    private Function httpParseSetupNextPipeline = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn){
            initialStage = true;
            wsConn.resetHttpReadValues();
            wsConn.resetContentAllRead();
            wsConn.setupNextPipeline();
            return StateQueueResult.STATE_RESULT_COMPLETE;
        }
    };

    public HttpParsePipelineMgr(WebServerConnState connState) {

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
        httpParseStateMachine = new StateMachine();
        httpParseStateMachine.addStateEntry(ConnectionStateEnum.INITIAL_SETUP, new StateEntry(httpParseInitialSetup));
        httpParseStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry(httpParseCheckSlowConnection));
        httpParseStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_HTTP_BUFFER, new StateEntry(httpParseAllocHttpBuffer));
        httpParseStateMachine.addStateEntry(ConnectionStateEnum.READ_HTTP_BUFFER, new StateEntry(httpParseReadHttpBuffer));
        httpParseStateMachine.addStateEntry(ConnectionStateEnum.PARSE_HTTP_BUFFER, new StateEntry(httpParseHttpBuffer));
        httpParseStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry(httpParseConnFinished));
        httpParseStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry(httpParseSetupNextPipeline));
        httpParseStateMachine.addStateEntry(ConnectionStateEnum.SEND_FINAL_RESPONSE, new StateEntry(httpParseSendXferResponse));
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
            result = httpParseStateMachine.stateMachineExecute(connectionState, nextVerb);

        } while (result == StateQueueResult.STATE_RESULT_CONTINUE);

        return result;
    }
}
