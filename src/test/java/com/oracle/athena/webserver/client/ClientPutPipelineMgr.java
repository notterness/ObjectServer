package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.connectionstate.ConnectionPipelineMgr;
import com.oracle.athena.webserver.connectionstate.ConnectionStateEnum;
import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;

import java.util.function.Function;

public class ClientPutPipelineMgr extends ConnectionPipelineMgr {

    private final ClientConnState connectionState;
    private boolean initialStage;

    private Function<ClientConnState, StateQueueResult> clientPutInitialSetup = conn -> {
        conn.setupInitial();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<ClientConnState, StateQueueResult> clientPutRequestDataBuffers = conn -> {
        if (conn.allocContentReadBuffers() == 0) {
            return StateQueueResult.STATE_RESULT_WAIT;
        } else {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function<ClientConnState, StateQueueResult> clientPutReadDataBuffers = conn -> {
        conn.readIntoDataBuffers();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<ClientConnState, StateQueueResult> clientPutReadCb = conn -> {
        conn.readClientBufferCallback();
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<ClientConnState, StateQueueResult> clientPutConnFinished = conn -> {
        initialStage = true;
        conn.reset();
        return StateQueueResult.STATE_RESULT_FREE;
    };

    private Function<ClientConnState, StateQueueResult> clientPutCheckSlowConn = conn -> {
        if (conn.checkSlowClientChannel()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        } else {
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    private Function<ClientConnState, StateQueueResult> clientPutProcessReadError = conn -> {
        if (conn.processReadErrorQueue()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };


    ClientPutPipelineMgr(ClientConnState connectionState) {
        super(connectionState, new StateMachine<>());
        this.connectionState = connectionState;

        initialStage = true;

        this.connectionState.memoryBuffersAreAvailable();
        this.connectionState.resetRequestedDataBuffers();
        this.connectionState.resetBuffersWaiting();
        this.connectionState.resetDataBufferReadsCompleted();

        this.connectionState.resetContentAllRead();

        this.connectionState.resetClientCallbackCompleted();

        connectionStateMachine.addStateEntry(ConnectionStateEnum.INITIAL_SETUP, new StateEntry<>(clientPutInitialSetup));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_CONTENT_DATA_BUFFER, new StateEntry<>(clientPutRequestDataBuffers));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.READ_CONTENT_DATA, new StateEntry<>(clientPutReadDataBuffers));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CLIENT_READ_CB, new StateEntry<>(clientPutReadCb));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry<>(clientPutConnFinished));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry<>(clientPutCheckSlowConn));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_READ_ERROR, new StateEntry<>(clientPutProcessReadError));
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
        if (!connectionState.outOfMemory() && connectionState.needsMoreContentBuffers()) {
            return ConnectionStateEnum.ALLOC_CONTENT_DATA_BUFFER;
        }

        /*
         ** The NIO.2 AsynchronousChannelRead can only have a single outstanding read at at time.
         **
         ** TODO: Support the NIO.2 read that can be passed in an array of ByteBuffers
         */
        if (connectionState.dataBuffersWaitingForRead()) {
            return ConnectionStateEnum.READ_CONTENT_DATA;
        }

        if (connectionState.getDataBufferReadsCompleted() > 0) {
            return ConnectionStateEnum.CLIENT_READ_CB;
        }

        /*
         ** Check if there are buffers in error that need to be processed.
         */
        if (connectionState.readErrorQueueNotEmpty()) {
            return ConnectionStateEnum.PROCESS_READ_ERROR;
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
}
