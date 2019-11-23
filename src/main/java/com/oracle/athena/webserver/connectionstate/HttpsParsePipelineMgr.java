package com.oracle.athena.webserver.connectionstate;


import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.eclipse.jetty.http.HttpStatus;

import javax.net.ssl.SSLException;
import java.util.function.Function;

class HttpsParsePipelineMgr extends ConnectionPipelineMgr {

    private WebServerConnState connectionState;

    private boolean initialStage;

    private StateMachine httpsParseStateMachine;


    private Function httpsParseInitialSetup = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.setupInitial();
            wsConn.setRequestedHttpBuffers(1);
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpsParseAllocHttpsBuffer = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            if (wsConn.allocHttpsBufferState() == 0){
                return StateQueueResult.STATE_RESULT_WAIT;
            }
            else {
                return StateQueueResult.STATE_RESULT_CONTINUE;
            }
        }
    };

    private Function httpsParseReadHttpsBuffer = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.readIntoMultipleBuffers();
            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
    };

    private Function httpsParseUnwrapHttpsBuffer = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.sslReadUnwrap();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpsParseHttpsBuffer = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.parseHttp();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpsParseConnFinished = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            initialStage = true;

            connectionState.resetHttpReadValues();
            connectionState.resetContentAllRead();
            return StateQueueResult.STATE_RESULT_FREE;
        }
    };

    private Function httpsParseCheckSlowConnection = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            //if (wsConn.timeoutChecker.inactivityThresholdReached()) {

            //} else return
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function httpsParseSendXferResponse = new Function<WebServerConnState, StateQueueResult>() {
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.sendResponse(HttpStatus.OK_200);
            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
    };

    private Function httpsParseSetupNextPipeline = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn){
            initialStage = true;
            connectionState.resetHttpReadValues();
            connectionState.resetContentAllRead();
            wsConn.setupNextPipeline();
            return StateQueueResult.STATE_RESULT_COMPLETE;
        }
    };

    public HttpsParsePipelineMgr(WebServerConnState connState) {

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

        httpsParseStateMachine = new StateMachine();
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.INITIAL_SETUP, new StateEntry(httpsParseInitialSetup));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry(httpsParseCheckSlowConnection));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_HTTP_BUFFER, new StateEntry(httpsParseAllocHttpsBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.READ_HTTP_BUFFER, new StateEntry(httpsParseReadHttpsBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.UNWRAP_HTTP_BUFFER, new StateEntry(httpsParseUnwrapHttpsBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.PARSE_HTTP_BUFFER, new StateEntry(httpsParseHttpsBuffer));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry(httpsParseConnFinished));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry(httpsParseSetupNextPipeline));
        httpsParseStateMachine.addStateEntry(ConnectionStateEnum.SEND_XFR_DATA_RESP, new StateEntry(httpsParseSendXferResponse));
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
         ** Are there completed reads, ready to unwrap
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
