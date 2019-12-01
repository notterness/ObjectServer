package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;

import java.util.function.Function;

public class AuthenticatePipelineMgr extends ConnectionPipelineMgr {
    private final WebServerConnState connectionState;
    private boolean initialStage;
    private boolean authenticateCheckCompleted;

    private Function<WebServerConnState, StateQueueResult> authenticateCheckEmbargo = wsConn -> {
        wsConn.checkEmbargo();
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerConnState, StateQueueResult> authenticateRequest = wsConn -> {
        wsConn.authenticateRequest();
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerConnState, StateQueueResult> authenticateFinished = wsConn -> {
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerConnState, StateQueueResult> authenticateFailedSendResponse = wsConn -> {
        wsConn.sendResponse(wsConn.getHttpParseStatus());

        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerConnState, StateQueueResult> authenticateResponseSent = wsConn -> {
        /*
        ** Cleanup from the response send and then return back to the previous pipeline
         */
        wsConn.processResponseWriteDone();

        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerConnState, StateQueueResult> checkSlowChannel = wsConn -> {
        if (wsConn.checkSlowClientChannel()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        } else {
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    AuthenticatePipelineMgr(WebServerConnState connectionState) {
        super(connectionState, new StateMachine<>());
        this.connectionState = connectionState;

        this.initialStage = true;
        this.authenticateCheckCompleted = false;

        /*
         ** Reset the state of the pipeline
         */
        this.connectionState.resetResponses();

        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_EMBARGO,
                new StateEntry<>(authenticateCheckEmbargo));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.AUTHENTICATE_REQUEST,
                new StateEntry<>(authenticateRequest));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.AUTHENTICATE_FINISHED,
                new StateEntry<>(authenticateFinished));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SEND_FINAL_RESPONSE,
                new StateEntry<>(authenticateFailedSendResponse));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND,
                new StateEntry<>(authenticateResponseSent));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL,
                new StateEntry<>(checkSlowChannel));

    }

    /*
     ** This determines the pipeline stages used to read in the content data.
     */
    @Override
    public ConnectionStateEnum nextPipelineStage() {

        /*
         ** First stage is to call the authenticate routines for this connection
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.CHECK_EMBARGO;
        }

        if (!authenticateCheckCompleted) {
            AuthenticateResultEnum result = connectionState.getAuthenticateResult();
            switch (result) {
                case EMBARGO_CHECK_IN_PROGRESS:
                case AUTHENTICATE_IN_PROGRESS:
                    return ConnectionStateEnum.CHECK_SLOW_CHANNEL;

                case EMBARGO_PASSED:
                    return ConnectionStateEnum.AUTHENTICATE_REQUEST;

                case AUTHENTICATE_PASSED:
                    authenticateCheckCompleted = true;
                    return ConnectionStateEnum.AUTHENTICATE_FINISHED;

                case EMBARGO_FAILED:
                case AUTHENTICATE_FAILED:
                    authenticateCheckCompleted = true;
                    if (!connectionState.hasFinalResponseBeenSent()) {
                        return ConnectionStateEnum.SEND_FINAL_RESPONSE;
                    }
                    break;
            }
        }

        /*
        ** Since the authentication failed, send out the final response to the client with the
        **   appropriate error.
         */
        if (!connectionState.hasFinalResponseBeenSent()) {
            return ConnectionStateEnum.SEND_FINAL_RESPONSE;
        }

        if (connectionState.getResponseChannelWriteDone()) {
            return ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND;
        }

        /*
         ** The status has been sent so cleanup the connection
         */
        if (connectionState.finalResponseSent()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }
}
