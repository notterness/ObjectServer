package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;

/**
 * A ConnectionPipelineMgr is a manager of pipes that enforces all pipeline managers to provide a mechanism for
 * executing and advancing the pipeline.
 */
public abstract class ConnectionPipelineMgr {

    private final ConnectionState connectionState;
    protected final StateMachine<ConnectionState, ConnectionStateEnum> connectionStateMachine;

    public ConnectionPipelineMgr(
            final ConnectionState connectionState,
            final StateMachine<ConnectionState, ConnectionStateEnum> connectionStateMachine) {
        this.connectionStateMachine = connectionStateMachine;
        this.connectionState = connectionState;
    }

    /**
     * Determines the next pipeline to advance to based upon the overall connection pipeline.
     *
     * @return a {@link ConnectionStateEnum} representing the next state to advance to.
     */
    public abstract ConnectionStateEnum nextPipelineStage();

    /**
     * Executes the current pipeline step.
     *
     * @return a {@link StateQueueResult} representing which state to advance the state machine to.
     */
    public StateQueueResult executePipeline() {
        StateQueueResult result;
        do {
            ConnectionStateEnum nextVerb = nextPipelineStage();
            result = connectionStateMachine.stateMachineExecute(connectionState, nextVerb);

        } while (result == StateQueueResult.STATE_RESULT_CONTINUE);
        return result;
    }
}
