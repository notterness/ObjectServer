package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.eclipse.jetty.http.HttpStatus;

import java.util.function.Function;

/*
** Description of how the HTTP Parse Pipeline works
**
**   The HTTP Parse Pipeline consists of the following stages:
**
**     1) INITIAL_SETUP
**          RUNS ON WORKER THREAD
**          Sets up the allocation for a single HTTP buffer using
**             requestedHttpBuffers - int
**
**     2) allocHttpBuffers()
**           RUNS ON WORKER THREAD
**           Checks if buffers are requested and if so, allocates them
**              checks requestedHttpBuffers - int
**           adds buffers to
**              allocatedHttpBufferQueue - LinkedList<BufferState>
**              allocatedHttpBufferCount - int (increment)
**              requestedHttpBuffers - int (decrement)
**
**     3) readIntoHttpBuffers()
**           RUNS ON WORKER THREAD
**           Checks if buffers have been allocated and if so reads into them
**              checks allocatedHttpBufferCount - int
**           Sets the number of outstanding reads
**              outstandingHttpReadCount - int (increment)
**              allocatedHttpBuffers - int (decrement)
**
**     3a) httpReadCompleted()
**            RUNS ON NIO.2 THREAD - CALLBACK FUNCTION
**            Moves BufferState onto read completed queue
**              httpReadDoneQueue - BlockingQueue<BufferState> (offer)
**              httpBufferReadsCompleted - AtomicInteger (increment)
**
**     4) parseHttpBuffers()
**           RUNS ON WORKER THREAD
**           Checks if there are BufferState with data in them ready to be parsed and if so,
**             feeds the data into the Jetty parser.
**             httpBufferReadsComplete - AtomicInteger (check and decrement)
**             httpReadDoneQueue - BlockingQueue<BufferState> (iterate and remove)
**             outstandingHttpReadCount - int (decrement)
**           Releases BufferState back to the free pool
**
**     5) setupNextPipeline()
**           RUNS ON WORKER THREAD
**           Updates the information needed to begin reading in the content data and
**             sets the next pipeline up.
 */
class HttpParsePipelineMgr extends ConnectionPipelineMgr {

    private final WebServerConnState connectionState;
    private boolean initialStage;

    private Function<WebServerConnState, StateQueueResult> httpParseInitialSetup = wsConn -> {
        wsConn.setupInitial();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerConnState, StateQueueResult> httpParseAllocHttpBuffer = wsConn -> {
        if (wsConn.allocHttpBuffer() == 0) {
            return StateQueueResult.STATE_RESULT_WAIT;
        } else {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function<WebServerConnState, StateQueueResult> httpParseReadHttpBuffer = wsConn -> {
        wsConn.readIntoHttpBuffers();
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<WebServerConnState, StateQueueResult> httpParseHttpBuffer = wsConn -> {
        wsConn.parseHttpBuffers();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerConnState, StateQueueResult> httpParseConnFinished = wsConn -> {
        initialStage = true;
        wsConn.reset();
        return StateQueueResult.STATE_RESULT_FREE;
    };

    private Function<WebServerConnState, StateQueueResult> httpParseAuthenticate = wsConn -> {
        wsConn.startAuthenticatePipeline();
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerConnState, StateQueueResult> httpParseCheckSlowConnection = wsConn -> {
        if (wsConn.checkSlowClientChannel()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        } else {
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    private Function<WebServerConnState, StateQueueResult> httpParseSendXferResponse = wsConn -> {
        wsConn.sendResponse(wsConn.getHttpParseStatus());
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerConnState, StateQueueResult> httpParseSetupNextPipeline = wsConn -> {
        initialStage = true;
        wsConn.resetHttpReadValues();
        wsConn.resetContentAllRead();
        wsConn.setupNextPipeline();
        return StateQueueResult.STATE_RESULT_COMPLETE;
    };

    private Function<WebServerConnState, StateQueueResult> httpParseProcessReadError = wsConn -> {
        if (wsConn.processReadErrorQueue()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<WebServerConnState, StateQueueResult> httpParseProcessFinalResponseSend = wsConn -> {
        wsConn.processResponseWriteDone();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    public HttpParsePipelineMgr(WebServerConnState connectionState) {
        super(connectionState, new StateMachine<>());
        this.connectionState = connectionState;

        initialStage = true;

        /*
         ** Reset the state of the pipeline
         */
        this.connectionState.resetHttpReadValues();

        /*
         ** This must be set to false here as there may be content data included in a buffer used to
         **   to read in the HTTP headers.
         */
        this.connectionState.resetContentAllRead();

        /*
         ** In error cases, this pipeline will send out error responses.
         */
        this.connectionState.resetResponses();
        connectionStateMachine.addStateEntry(ConnectionStateEnum.INITIAL_SETUP, new StateEntry<>(httpParseInitialSetup));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry<>(httpParseCheckSlowConnection));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_HTTP_BUFFER, new StateEntry<>(httpParseAllocHttpBuffer));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.READ_HTTP_BUFFER, new StateEntry<>(httpParseReadHttpBuffer));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PARSE_HTTP_BUFFER, new StateEntry<>(httpParseHttpBuffer));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.RUN_AUTHENTICATION_PIPELINE, new StateEntry<>(httpParseAuthenticate));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry<>(httpParseConnFinished));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry<>(httpParseSetupNextPipeline));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SEND_FINAL_RESPONSE, new StateEntry<>(httpParseSendXferResponse));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_READ_ERROR, new StateEntry<>(httpParseProcessReadError));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND, new StateEntry<>(httpParseProcessFinalResponseSend));
    }

    /*
     ** This determines the pipeline stages used to read in and parse the HTTP headers.
     */
    @Override
    public ConnectionStateEnum nextPipelineStage() {

        /*
         ** Perform the initial setup
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.INITIAL_SETUP;
        }

        /*
         ** Are there outstanding buffers to be allocated. If the code had attempted to allocate
         **   buffers and failed, check if there is other work to do. No point trying the buffer
         **   allocation right away.
         */
        if (!connectionState.outOfMemory() && connectionState.httpBuffersNeeded()) {
            return ConnectionStateEnum.ALLOC_HTTP_BUFFER;
        }

        /*
         ** Are there buffers waiting to have a read performed?
         */
        if (connectionState.httpBuffersAllocated()) {
            return ConnectionStateEnum.READ_HTTP_BUFFER;
        }

        /*
         ** Are there completed reads, priority is processing the HTTP header
         */
        if (connectionState.httpBuffersReadyForParsing()) {
            return ConnectionStateEnum.PARSE_HTTP_BUFFER;
        }

        /*
         ** Check if there are buffers in error that need to be processed.
         */
        if (connectionState.readErrorQueueNotEmpty()) {
            return ConnectionStateEnum.PROCESS_READ_ERROR;
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **    reads.
         */
        if (connectionState.hasChannelFailed()) {
            if (connectionState.outstandingHttpBufferReads() == 0) {
                return ConnectionStateEnum.CONN_FINISHED;
            }

            // TODO: Need to log something to indicate waiting for reads to complete
            return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
        }

        if (connectionState.getResponseChannelWriteDone()) {
            return ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND;
        }

        /*
         ** The status has been sent so cleanup the connection. This check needs to proceed any checks
         **   that will cause a response to be sent.
         */
        if (connectionState.finalResponseSent()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        /*
         ** Check if there has been an HTTP headers parsing error and if so, return the appropriate
         **    response to the client.
         */
        if (connectionState.getHttpParseStatus() != HttpStatus.OK_200) {
            return ConnectionStateEnum.SEND_FINAL_RESPONSE;
        }

        AuthenticateResultEnum authenticateResult = connectionState.authenticationComplete();
        if (authenticateResult != AuthenticateResultEnum.INVALID_RESULT) {
            /*
             ** Figure out how many buffers to read (meaning setup the ContentReadPipelineMgr)
             */
            return ConnectionStateEnum.SETUP_NEXT_PIPELINE;
        }

        /*
         ** Check if the header parsing is completed and if so, setup the initial reads
         **   for the content.
         **
         */
        if (connectionState.httpHeadersParsed()) {
            /*
             ** Figure out how many buffers to read.
             */
            return ConnectionStateEnum.RUN_AUTHENTICATION_PIPELINE;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }
}
