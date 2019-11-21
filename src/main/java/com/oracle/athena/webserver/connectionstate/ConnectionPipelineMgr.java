package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateQueueResult;

abstract public class ConnectionPipelineMgr {

    public ConnectionPipelineMgr(ConnectionState connState) {

    }

    abstract public ConnectionStateEnum nextPipelineStage();

    abstract public StateQueueResult executePipeline();
}
