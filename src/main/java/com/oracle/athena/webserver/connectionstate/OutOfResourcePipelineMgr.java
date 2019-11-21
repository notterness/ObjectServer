package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;

import java.util.function.Function;

public class OutOfResourcePipelineMgr extends ConnectionPipelineMgr {
    private WebServerConnState connectionState;

    private boolean initialStage;

    private StateMachine outOfResourceStateMachine;

    private Function outOfResourceSendResponse = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.setupInitial();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function outOfResourceConnFinished = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            initialStage = true;

            wsConn.reset();

            return StateQueueResult.STATE_RESULT_FREE;
        }
    };

    private Function outOfResourceCheckSlowConnection = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {

            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    OutOfResourcePipelineMgr(WebServerConnState connState) {

        super(connState);

        this.connectionState = connState;

        initialStage = true;

        /*
         ** Reset the state of the pipeline
         */
        connectionState.resetResponses();

        outOfResourceStateMachine = new StateMachine();
        outOfResourceStateMachine.addStateEntry( ConnectionStateEnum.SEND_OUT_OF_RESOURCE_RESPONSE,
                new StateEntry(outOfResourceSendResponse));
        outOfResourceStateMachine.addStateEntry( ConnectionStateEnum.CONN_FINISHED, new StateEntry(outOfResourceConnFinished));
        outOfResourceStateMachine.addStateEntry( ConnectionStateEnum.CHECK_SLOW_CHANNEL,
                new StateEntry( outOfResourceCheckSlowConnection));

    }

    /*
     ** This determines the pipeline stages used to read in the content data.
     */
    public ConnectionStateEnum nextPipelineStage() {

        System.out.println("OutOfResourcePipelineMgr[" + connectionState.getConnStateId() + "] initialState: " + initialStage);

        /*
         ** First setup to perform the content reads. This is required since the buffer used to read in the
         **   HTTP headers may have also had data for the content at the end of it.
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.SEND_OUT_OF_RESOURCE_RESPONSE;
        }

        /*
         ** Check if there was a channel error and cleanup if there was as there is not point waiting for
         **   the write to complete
         */
        if (connectionState.hasChannelFailed()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        /*
         ** The status has been sent so cleanup the connection
         */
        if (connectionState.finalResponseSent()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }

    @Override
    public StateQueueResult executePipeline() {
        StateQueueResult result;
        ConnectionStateEnum nextVerb;

        do {
            nextVerb = nextPipelineStage();
            result = outOfResourceStateMachine.stateMachineExecute(connectionState, nextVerb);

        } while (result == StateQueueResult.STATE_RESULT_CONTINUE);

        return result;
    }
}