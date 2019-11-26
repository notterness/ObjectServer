package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateQueueResult;

/**
 * A ConnectionPipelineMgr is a manager of pipes that enforces all pipeline managers to provide a mechanism for
 * executing and advancing the pipeline.
 */
public interface ConnectionPipelineMgr {

    /**
     * Determines the next pipeline to advance to based upon the overall connection pipeline.
     * @return a {@link ConnectionStateEnum} representing the next state to advance to.
     */
    ConnectionStateEnum nextPipelineStage();

    /**
     * Executes the current pipeline step.
     * @return a {@link StateQueueResult} representing which state to advance the state machine to.
     */
    StateQueueResult executePipeline();
}
