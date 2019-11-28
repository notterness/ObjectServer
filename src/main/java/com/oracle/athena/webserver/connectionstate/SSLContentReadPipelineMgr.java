package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class SSLContentReadPipelineMgr extends ConnectionPipelineMgr {

    private static final Logger LOG = LoggerFactory.getLogger(SSLContentReadPipelineMgr.class);

    private final WebServerSSLConnState connectionState;

    private boolean initialStage;

    private Function<WebServerSSLConnState, StateQueueResult> contentReadSetup = wsConn -> {
        wsConn.determineNextContentRead();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadRequestDataBuffers = wsConn -> {
        if (wsConn.allocClientReadBufferState() == 0) {
            return StateQueueResult.STATE_RESULT_WAIT;
        } else {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadDataBuffers = wsConn -> {
        wsConn.readIntoDataBuffers();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadUnwrap = wsConn ->  {
            wsConn.unwrapData();
            return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadCheckSlowChannel = wsConn -> {
        if (wsConn.checkSlowClientChannel()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        } else {
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadSendXferResponse = wsConn -> {
        wsConn.sendResponse(wsConn.getHttpParseStatus());
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadConnFinished = wsConn ->{
        wsConn.releaseContentBuffers();
        initialStage = true;
        wsConn.reset();
        return StateQueueResult.STATE_RESULT_FREE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadSetNextPipeline = wsConn -> {
        initialStage = true;
        wsConn.resetRequestedDataBuffers();
        wsConn.resetBuffersWaiting();
        wsConn.resetDataBufferReadsCompleted();
        wsConn.resetResponses();
        return StateQueueResult.STATE_RESULT_COMPLETE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadProcessReadError = wsConn -> {
        if (wsConn.processReadErrorQueue()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadProcessFinalResponseSend = wsConn ->{
        wsConn.processResponseWriteDone();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentReadReleaseBuffers = wsConn -> {
        wsConn.releaseContentBuffers();
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentCalculateMd5 = wsConn -> {
        /*
        **  the MD5 threads are busy - requeue
        if (wsConn.sendBuffersToMd5Worker() == 0) {
            return StateQueueResult.STATE_RESULT_REQUEUE;
        }
        */
        wsConn.sendBuffersToMd5Worker();
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerSSLConnState, StateQueueResult> contentMd5Complete = wsConn -> {
        wsConn.md5CalculateComplete();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    SSLContentReadPipelineMgr(WebServerSSLConnState connectionState) {
        super(connectionState, new StateMachine<>());
        this.connectionState = connectionState;

        initialStage = true;

        /*
         ** Reset the state of the pipeline
         */
        connectionState.resetRequestedDataBuffers();
        connectionState.resetBuffersWaiting();
        connectionState.resetDataBufferReadsCompleted();

        connectionState.resetResponses();
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SETUP_CONTENT_READ, new StateEntry<>(contentReadSetup));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER, new StateEntry<>(contentReadRequestDataBuffers));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.READ_CLIENT_DATA, new StateEntry<>(contentReadDataBuffers));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry<>(contentReadCheckSlowChannel));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SEND_FINAL_RESPONSE, new StateEntry<>(contentReadSendXferResponse));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry<>(contentReadConnFinished));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry<>(contentReadSetNextPipeline));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_READ_ERROR, new StateEntry<>(contentReadProcessReadError));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND, new StateEntry<>(contentReadProcessFinalResponseSend));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.RELEASE_CONTENT_BUFFERS, new StateEntry<>(contentReadReleaseBuffers));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.MD5_CALCULATE, new StateEntry<>(contentCalculateMd5));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.MD5_CALCULATE_COMPLETE, new StateEntry<>(contentMd5Complete));
    }

    /*
     ** This determines the pipeline stages used to read in the content data.
     */
    @Override
    public ConnectionStateEnum nextPipelineStage() {

        /*
         ** First setup to perform the content reads. This is required since the buffer used to read in the
         **   HTTP headers may have also had data for the content at the end of it.
         ** The is the last thing to check prior to having the channel wait as it should only happen once.
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
        if (connectionState.dataBuffersWaitingForRead()) {
            return ConnectionStateEnum.READ_CLIENT_DATA;
        }

        if (connectionState.getDataBuffersUnwrapRequired() > 0) {
            return ConnectionStateEnum.UNWRAP_DATA_BUFFER;
        }

        if (connectionState.getResponseChannelWriteDone()) {
            return ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND;
        }

        /*
         ** Check if there are buffers in error that need to be processed.
         */
        if (connectionState.readErrorQueueNotEmpty()) {
            return ConnectionStateEnum.PROCESS_READ_ERROR;
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **   reads. The channel failure will be set in the PROCESS_READ_ERROR state.
         */
        if (connectionState.hasChannelFailed() && connectionState.getDataBufferDigestSent() == 0) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        if (connectionState.getDataBufferReadsCompleted() > 0) {
            return ConnectionStateEnum.MD5_CALCULATE;
        }

        if (connectionState.hasAllContentBeenRead() && !connectionState.getDigestComplete() &&
                connectionState.getDataBufferReadsCompleted() == 0 &&
                connectionState.getDataBufferDigestSent() == 0) {
            return ConnectionStateEnum.MD5_CALCULATE_COMPLETE;
        }
        /*
         ** This is where the processing of the content data buffers will start. For now, this just
         **   goes directly to the release content buffers stage, which only releases the buffers back to the free
         **   pool.
         */
        if (connectionState.getDataBufferReadsCompleted() > 0) {
            return ConnectionStateEnum.RELEASE_CONTENT_BUFFERS;
        }

        /*
         ** Check if the content has all been read in and then proceed to finishing the processing
         **
         ** TODO: Start adding in the steps to process the content data instead of just sending status
         */
        if (connectionState.hasAllContentBeenRead() && !connectionState.hasFinalResponseBeenSent()) {
            return ConnectionStateEnum.SEND_FINAL_RESPONSE;
        }

        /*
         ** TODO: This is not really the exit point for the state machine, but until the
         **   steps for dealing with user data are added it is.
         */
        if (connectionState.finalResponseSent()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        /*
         ** First setup to perform the content reads. This is required since the buffer used to read in the
         **   HTTP headers may have also had data for the content at the end of it.
         ** The is the last thing to check prior to having the channel wait as it should only happen once.
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.SETUP_CONTENT_READ;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }
}