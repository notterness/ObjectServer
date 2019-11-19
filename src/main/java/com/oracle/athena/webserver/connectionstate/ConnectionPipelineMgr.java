package com.oracle.athena.webserver.connectionstate;

abstract public class ConnectionPipelineMgr {

    public ConnectionPipelineMgr(ConnectionState connState) {

    }

    abstract public ConnectionStateEnum nextPipelineStage();
}
