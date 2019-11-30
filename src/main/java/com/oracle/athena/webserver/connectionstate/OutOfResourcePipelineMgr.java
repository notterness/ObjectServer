package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.eclipse.jetty.http.HttpStatus;

import java.util.function.Function;

public class OutOfResourcePipelineMgr extends ConnectionPipelineMgr {
    private final WebServerConnState connectionState;
    private boolean initialStage;

    private Function<WebServerConnState, StateQueueResult> outOfResourceSendResponse = wsConn -> {
        wsConn.sendResponse(HttpStatus.TOO_MANY_REQUESTS_429);
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerConnState, StateQueueResult> outOfResourceConnFinished = wsConn -> {
        initialStage = true;
        wsConn.reset();
        return StateQueueResult.STATE_RESULT_FREE;
    };

    private Function<WebServerConnState, StateQueueResult> outOfResourceCheckSlowConnection = wsConn -> {
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };


    private Function<WebServerConnState, StateQueueResult> OutOfResourceProcessFinalResponseSend = wsConn -> {
        wsConn.processResponseWriteDone();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    OutOfResourcePipelineMgr(WebServerConnState connectionState) {
        super(connectionState, new StateMachine<>());
        this.connectionState = connectionState;

        initialStage = true;

        /*
         ** Reset the state of the pipeline
         */
        this.connectionState.resetResponses();

        connectionStateMachine.addStateEntry(ConnectionStateEnum.SEND_OUT_OF_RESOURCE_RESPONSE,
                new StateEntry<>(outOfResourceSendResponse));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry<>(outOfResourceConnFinished));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL,
                new StateEntry<>(outOfResourceCheckSlowConnection));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND,
                new StateEntry<>(OutOfResourceProcessFinalResponseSend));

    }

    /*
     ** This determines the pipeline stages used to read in the content data.
     */
    @Override
    public ConnectionStateEnum nextPipelineStage() {

        /*
         ** First setup to perform the content reads. This is required since the buffer used to read in the
         **   HTTP headers may have also had data for the content at the end of it.
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.SEND_OUT_OF_RESOURCE_RESPONSE;
        }

        /*
         ** Check if there was a channel error and cleanup if there was as there is not point waiting for
         **   the write to complete
         */
        if (connectionState.hasChannelFailed()) {
            return ConnectionStateEnum.CONN_FINISHED;
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