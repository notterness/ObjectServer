package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.http.CasperHttpInfo;
import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.server.WebServerAuths;

import java.util.function.Function;

public class AuthenticatePipelineMgr extends ConnectionPipelineMgr {

    /*
     ** The WebServerAuths are setup when the WebServer class in instantiated. They are used
     **   for all connections.
     */
    private final WebServerAuths webServerAuths;
    private final WebServerConnState connectionState;
    private final CasperHttpInfo casperHttpInfo;
    private boolean initialStage;
    private boolean authenticateCheckCompleted;

    private Function<WebServerConnState, StateQueueResult> authenticateResponseSent = wsConn -> {
        /*
         ** Cleanup from the response send and then return back to the previous pipeline
         */
        wsConn.processResponseWriteDone();

        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerConnState, StateQueueResult> authenticateComplete = wsConn -> {
        wsConn.authenticatePipelineCompleteCb(AuthenticateResultEnum.AUTHENTICATE_PASSED);

        /*
        ** This pipeline is finished.
         */
        return StateQueueResult.STATE_RESULT_FREE;
    };

    private Function<WebServerConnState, StateQueueResult> checkSlowChannel = wsConn -> {
        if (wsConn.checkSlowClientChannel()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        } else {
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    AuthenticatePipelineMgr(
            final WebServerConnState connectionState,
            final CasperHttpInfo casperHttpInfo,
            final WebServerAuths webServerAuths) {
        super(connectionState, new StateMachine<>());
        this.connectionState = connectionState;
        this.webServerAuths = webServerAuths;
        this.casperHttpInfo = casperHttpInfo;
        this.initialStage = true;
        this.authenticateCheckCompleted = false;

        /*
         ** Reset the state of the pipeline
         */
        this.connectionState.resetResponses();

        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_EMBARGO,
                new StateEntry<>(wsConn -> checkEmbargo()));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.AUTHENTICATE_REQUEST,
                new StateEntry<>(wsConn -> authenticate()));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.AUTHENTICATE_FINISHED,
                new StateEntry<>(authenticateComplete));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SEND_FINAL_RESPONSE,
                new StateEntry<>(wsConn -> {
                    connectionState.sendResponse(connectionState.getHttpParseStatus());
                    return StateQueueResult.STATE_RESULT_WAIT;
                }));
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

            //return ConnectionStateEnum.CHECK_EMBARGO;
            authenticateCheckCompleted = true;

            return ConnectionStateEnum.AUTHENTICATE_FINISHED;
        }

        if (!authenticateCheckCompleted) {
            AuthenticateResultEnum result = getAuthenticateResult();
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

    /**
     * This is used to perform the Embargo checking for this request
     */
    private StateQueueResult checkEmbargo() {
        final String namespace = casperHttpInfo.getNamespace();
        final String bucket = casperHttpInfo.getBucket();
        final String object = casperHttpInfo.getObject();

        final EmbargoV3Operation embargoV3Operation = EmbargoV3Operation.builder()
                .setApi(EmbargoV3Operation.Api.V2)
                .setOperation(CasperOperation.PUT_OBJECT)
                .setNamespace(namespace)
                .setBucket(bucket)
                .setObject(object)
                .build();
        webServerAuths.getEmbargoV3().enter(embargoV3Operation);
        return StateQueueResult.STATE_RESULT_WAIT;
    }

    /**
     * The performs the authentication for a HTTP request
     */
    private StateQueueResult authenticate() {
        // TODO Wire this up
        return StateQueueResult.STATE_RESULT_WAIT;
    }

    /**
     * This is used to return the various steps that authentication goes through and
     *   there status.
     *
     * TODO: Wire this through
     */
    private AuthenticateResultEnum getAuthenticateResult() {
        return AuthenticateResultEnum.AUTHENTICATE_PASSED;
    }
}
