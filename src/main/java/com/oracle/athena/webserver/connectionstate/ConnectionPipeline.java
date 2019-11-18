package com.oracle.athena.webserver.connectionstate;

abstract public class ConnectionPipeline {

    ConnectionPipeline(ConnectionState connState) {

    }

    abstract ConnectionStateEnum nextPipelineStage();
}
