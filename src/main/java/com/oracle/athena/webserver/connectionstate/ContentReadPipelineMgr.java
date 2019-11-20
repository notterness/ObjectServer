package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.eclipse.jetty.http.HttpStatus;

import java.util.function.Function;

public class ContentReadPipelineMgr extends ConnectionPipelineMgr {

    private WebServerConnState connectionState;

    private boolean initialStage;

    private StateMachine contentReadStateMachine;
    private Function contentReadSetup = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.determineNextContentRead();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };
    private Function contentReadRequestDataBuffers = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            if (wsConn.allocClientReadBufferState() == 0) {
                return StateQueueResult.STATE_RESULT_WAIT;
            } else {
                return StateQueueResult.STATE_RESULT_CONTINUE;
            }
        }
    };

    private Function contentReadDataBuffers = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.readIntoDataBuffers();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function contentReadCheckSlowChannel = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            //if (wsConn.timeoutChecker.inactivityThresholdReached()) {

            //} else return
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function contentReadSendXferResponse = new Function<WebServerConnState, StateQueueResult>() {
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.sendResponse(HttpStatus.OK_200);
            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
    };

    private Function contentReadConnFinished = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            initialStage = true;
            connectionState.resetRequestedDataBuffers();
            connectionState.resetBuffersWaiting();
            connectionState.resetDataBufferReadsCompleted();
            connectionState.resetResponses();
            return StateQueueResult.STATE_RESULT_FREE;
        }
    };

    private Function contentReadSetNextPipeline = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            initialStage = true;
            connectionState.resetRequestedDataBuffers();
            connectionState.resetBuffersWaiting();
            connectionState.resetDataBufferReadsCompleted();
            connectionState.resetResponses();
            return StateQueueResult.STATE_RESULT_COMPLETE;
        }
    };

    ContentReadPipelineMgr(WebServerConnState connState) {

        super(connState);

        this.connectionState = connState;

        initialStage = true;

        /*
         ** Reset the state of the pipeline
         */
        connectionState.resetRequestedDataBuffers();
        connectionState.resetBuffersWaiting();
        connectionState.resetDataBufferReadsCompleted();

        connectionState.resetResponses();
        contentReadStateMachine = new StateMachine();
        contentReadStateMachine.addStateEntry(ConnectionStateEnum.SETUP_CONTENT_READ, new StateEntry(contentReadSetup));
        contentReadStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER, new StateEntry(contentReadRequestDataBuffers));
        contentReadStateMachine.addStateEntry(ConnectionStateEnum.READ_CLIENT_DATA, new StateEntry(contentReadDataBuffers));
        contentReadStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL,new StateEntry(contentReadCheckSlowChannel));
        contentReadStateMachine.addStateEntry(ConnectionStateEnum.SEND_XFR_DATA_RESP, new StateEntry(contentReadSendXferResponse));
        contentReadStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry(contentReadConnFinished));
        contentReadStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry(contentReadSetNextPipeline));
    }

    /*
     ** This determines the pipeline stages used to read in the content data.
     */
    public ConnectionStateEnum nextPipelineStage() {

        /*
        ** First setup to perform the content reads. This is required since the buffer used to read in the
        **   HTTP headers may have also had data for the content at the end of it.
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.SETUP_CONTENT_READ;
        }

        /*
         ** Are there outstanding buffers to be allocated. If the code had attempted to allocate
         **   buffers and failed, check if there is other work to do. No point trying the buffer
         **   allocation right away.
         **
         */
        if (!connectionState.outOfMemory() && connectionState.needsMoreDataBuffers()) {
            return ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER;
        }

        /*
         ** The NIO.2 AsynchronousChannelRead can only have a single outstanding read at at time.
         **
         ** TODO: Support the NIO.2 read that can be passed in an array of ByteBuffers
         */
        if (connectionState.dataBuffersWaitingForRead()){
            return ConnectionStateEnum.READ_CLIENT_DATA;
        }

        /*
        ** Check if the content has all been read in and then proceed to finishing the processing
        **
        ** TODO: Start adding in the steps to process the content data instead of just sending status
         */
        if (connectionState.hasAllContentBeenRead() && !connectionState.hasFinalResponseBeenSent()) {
                return ConnectionStateEnum.SEND_XFR_DATA_RESP;
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **   reads.
         */
        if (connectionState.hasChannelFailed()) {
            if (connectionState.getDataBufferReadsCompleted() == 0) {
                return ConnectionStateEnum.CONN_FINISHED;
            }
        }

        /*
         ** TODO: This is not really the exit point for the state machine, but until the
         **   steps for dealing with user data are added it is.
         */
        if (connectionState.hasDataResponseBeenSent() || connectionState.finalResponseSent()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }

    public StateQueueResult executePipeline() {
        StateQueueResult result;
        ConnectionStateEnum nextVerb;

        do {
            nextVerb = nextPipelineStage();
            result = contentReadStateMachine.stateMachineExecute(connectionState, nextVerb);

        } while (result == StateQueueResult.STATE_RESULT_CONTINUE);

        return result;
    }

    public void reset() {
        initialStage = true;
    }
}