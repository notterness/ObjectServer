package com.oracle.athena.webserver.connectionstate;


import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;

import javax.net.ssl.SSLException;
import java.util.function.Function;

class SSLHandshakePipelineMgr extends ConnectionPipelineMgr {

    private WebServerConnState connectionState;

    private boolean initialStage;

    private StateMachine sslHandshakeStateMachine;


    private Function sslHandshakeInitialSetup = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.setupSSL();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function sslHandshakeAllocBuffers = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            StateQueueResult result = StateQueueResult.STATE_RESULT_CONTINUE;
            wsConn.allocSSLHandshakeBuffers();
            if (wsConn.isSSLBuffersNeeded() == true) {
                result = StateQueueResult.STATE_RESULT_WAIT;
            }
            else {

                try {
                    wsConn.beginHandshake();
                } catch (SSLException e) {
                    //FIXME: Handle this error condition
                }

            }

            return result;
        }
    };

    private Function sslHandshakeExec = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            wsConn.doSSLHandshake();
            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
    };

    //FIXME: handle connection finished
    private Function sslHandshakeConnFinished = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn) {
            initialStage = true;
            connectionState.freeSSLHandshakeBuffers();
            return StateQueueResult.STATE_RESULT_FREE;
        }
    };

    private Function sslHandshakeNextPipeline = new Function<WebServerConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(WebServerConnState wsConn){
            initialStage = true;
            connectionState.freeSSLHandshakeBuffers();
            wsConn.setupNextSSLPipeline();
            return StateQueueResult.STATE_RESULT_COMPLETE;
        }
    };

    public SSLHandshakePipelineMgr(WebServerConnState connState) {

        super(connState);

        connectionState = connState;

        initialStage = true;

        /*
        ** This must be set to false here as there may be content data included in a buffer used to
        **   to read in the HTTP headers.
         */
        sslHandshakeStateMachine = new StateMachine();
        sslHandshakeStateMachine.addStateEntry(ConnectionStateEnum.INITIAL_SETUP, new StateEntry(sslHandshakeInitialSetup));
        sslHandshakeStateMachine.addStateEntry(ConnectionStateEnum.SSL_ALLOC_BUFFERS, new StateEntry(sslHandshakeAllocBuffers));
        sslHandshakeStateMachine.addStateEntry(ConnectionStateEnum.SSL_HANDSHAKE, new StateEntry(sslHandshakeExec));
        sslHandshakeStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry(sslHandshakeNextPipeline));
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
         ** Check if handshaking is successfully completed.
         */
        if (connectionState.isSSLHandshakeSuccess()) {
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
        if (!connectionState.outOfMemory() && connectionState.isSSLBuffersNeeded()) {
            return ConnectionStateEnum.SSL_ALLOC_BUFFERS;
        }

        if (connectionState.isSSLHandshakeRequired()) {
            return ConnectionStateEnum.SSL_HANDSHAKE;
        }

        /*
         ** If it reaches here, close connection
         */
        return ConnectionStateEnum.CONN_FINISHED;
    }

    public StateQueueResult executePipeline() {
        StateQueueResult result;
        ConnectionStateEnum nextVerb;

        do {
            nextVerb = nextPipelineStage();
            result = sslHandshakeStateMachine.stateMachineExecute(connectionState, nextVerb);

        } while (result == StateQueueResult.STATE_RESULT_CONTINUE);

        return result;
    }
}