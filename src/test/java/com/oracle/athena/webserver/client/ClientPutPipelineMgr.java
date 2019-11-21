package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.connectionstate.ConnectionPipelineMgr;
import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.ConnectionStateEnum;
import com.oracle.athena.webserver.connectionstate.WebServerConnState;
import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;

import java.util.function.Function;

public class ClientPutPipelineMgr extends ConnectionPipelineMgr {

    private ClientConnState connectionState;

    private boolean initialStage;

    private StateMachine clientPutStateMachine;

    private Function clientPutInitialSetup = new Function<ClientConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(ClientConnState wsConn) {
            wsConn.setupInitial();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function clientPutRequestDataBuffers = new Function<ClientConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(ClientConnState conn) {
            if (conn.allocClientReadBufferState() == 0) {
                return StateQueueResult.STATE_RESULT_WAIT;
            } else {
                return StateQueueResult.STATE_RESULT_CONTINUE;
            }
        }
    };

    private Function clientPutReadDataBuffers = new Function<ClientConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(ClientConnState conn) {
            conn.readIntoDataBuffers();
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function clientPutReadCb = new Function<ClientConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(ClientConnState conn) {
            conn.readClientBufferCallback();
            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
    };

    private Function clientPutConnFinished = new Function<ClientConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(ClientConnState conn) {
            initialStage = true;
            conn.reset();
            return StateQueueResult.STATE_RESULT_FREE;
        }
    };

    private Function clientPutCheckSlowConn = new Function<ClientConnState, StateQueueResult>() {
        @Override
        public StateQueueResult apply(ClientConnState conn) {
            if (conn.checkSlowClientChannel()) {
                return StateQueueResult.STATE_RESULT_REQUEUE;
            } else {
                return StateQueueResult.STATE_RESULT_WAIT;
            }
        }
    };

    ClientPutPipelineMgr(ClientConnState connState) {

        super(connState);

        this.connectionState = connState;

        initialStage = true;

        connectionState.memoryBuffersAreAvailable();
        connectionState.resetRequestedDataBuffers();
        connectionState.resetBuffersWaiting();
        connectionState.resetDataBufferReadsCompleted();

        connectionState.resetContentAllRead();

        connectionState.resetClientCallbackCompleted();

        clientPutStateMachine = new StateMachine();
        clientPutStateMachine.addStateEntry(ConnectionStateEnum.INITIAL_SETUP, new StateEntry( clientPutInitialSetup));
        clientPutStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER, new StateEntry(clientPutRequestDataBuffers));
        clientPutStateMachine.addStateEntry(ConnectionStateEnum.READ_CLIENT_DATA, new StateEntry(clientPutReadDataBuffers));
        clientPutStateMachine.addStateEntry(ConnectionStateEnum.CLIENT_READ_CB, new StateEntry(clientPutReadCb));
        clientPutStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry(clientPutConnFinished));
        clientPutStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry(clientPutCheckSlowConn));
    }

    /*
     ** This determines the pipeline stages used to read in the content data.
     */
    public ConnectionStateEnum nextPipelineStage() {

        /*
         ** First setup to perform the content reads. This is required since the buffer used to read in the
         **   HTTP headers may have also had data for the conent at the end of it.
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.INITIAL_SETUP;
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

        if (connectionState.getDataBufferReadsCompleted() > 0) {
            return ConnectionStateEnum.CLIENT_READ_CB;
        }

        /*
         ** The check for clientCallbackCompleted needs to be before the contentAllRead check
         **   otherwise the Connection will get stuck in the CLIENT_READ_CB state.
         **
         ** TODO: Is there value to moving the repsonse parsing into this state machine to properly handle
         **   the setting of the contentAllRead flag. Currently, it is never sent as the "Content-Length" is
         **   never parsed out of the HTTP response.
         */
        if (connectionState.hasClientCallbackCompleted()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        /*
         ** Check if the content has all been read in and then proceed to finishing the processing
         **
         ** TODO: Start adding in the steps to process the content data instead of just sending status
         */
        if (connectionState.hasAllContentBeenRead()) {
            return ConnectionStateEnum.CLIENT_READ_CB;
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **    reads.
         */
        if (connectionState.hasChannelFailed()) {
            if (connectionState.dataReadsPending() == 0) {
                return ConnectionStateEnum.CONN_FINISHED;
            }
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }

    public StateQueueResult executePipeline() {
        StateQueueResult result = StateQueueResult.STATE_RESULT_COMPLETE;
        ConnectionStateEnum nextVerb;

        do {
            nextVerb = nextPipelineStage();
            result = clientPutStateMachine.stateMachineExecute(connectionState, nextVerb);

        } while (result == StateQueueResult.STATE_RESULT_CONTINUE);

        return result;
    }
}
