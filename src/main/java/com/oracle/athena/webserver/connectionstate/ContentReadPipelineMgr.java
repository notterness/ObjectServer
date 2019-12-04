package com.oracle.athena.webserver.connectionstate;

/*
** Description of how the Content Read Pipeline works
**
**   The HTTP Parse Pipeline consists of the following stages:
**
**     1) ALLOC_CONTENT_DATA_BUFFERS
**           RUNS ON THE WORKER THREAD
**           Attempts to allocate buffers to read in content data.
**              requestedDataBuffers - int (decrement)
**              allocatedDataBuffers - int (increment)
**              allocatedDataBufferQueue - LinkedList<BufferState>
**              contentBytesAllocated - AtomicLong
**
**    2) READ_CONTENT_DATA
**          RUNS ON THE WORKER THREAD
**          Reads data from the channel into the content data buffers
**             allocatedDataBuffers - int (decrement)
**             allocatedDataBufferQueue - LinkedList<BufferState>
**             outstandingDataReadCount - AtomicInteger (increment)
**
**    2a) dataReadCompleted()
**           RUNS ON NIO.2 THREAD - CALLBACK FUNCTION
**           Moves BufferState onto read completed queue
**             dataReadDoneQueue - BlockingQueue<BufferState> (offer)
**             outstandingDataReadCount - AtomicInteger (decrement)
**             dataBufferReadsCompleted - AtomicInteger (incremented in addDataBuffer())
**
**    3) MD5_CALCULATE - connectionState.sendBuffersToMd5Worker()
**          RUNS ON THE WORKER THREAD
**          Moves BufferState from the read completed queue to the MD5 threads queue.
**            dataReadDoneQueue - BlockingQueue<BufferState> (toArray, which should be a drainTo)
**            digestWorkQueue - BlockingQueue<BufferState> (offer)
**            dataBufferDigestSent - AtomicInteger (increment)
**
**    4) Compute the MD5
**          RUNS ON ONE OF THE ServerDigestThread(s)
**          This computes the accumulating MD5 digest for a particular BufferState
**             digestWorkQueue - BlockingQueue<BufferState> (poll and remove)
**             dataMd5DoneQueue - BlockingQueue<BufferState> (add, but should be offer)
**
**    5) MD5_BUFFER_DONE
**          RUNS ON THE WORKER THREAD
**          This pulls buffers that have had their MD5 calculation completed off the queue and ...
**            dataMd5DoneQueue - BlockingQueue<BufferState> (drainTo)
**            dataBufferDigestCompleted - AtomicInteger (increment)
**            dataBufferDigestSent - AtomicInteger (decrement)
**
**    6) MD5_CALCULATE_COMPLETE
**          Pulls the final MD5 value from the calculation and compares it to the value passed
**             in through the header.
**             digestComplete() - AtomicBoolean (set)
*/

import com.oracle.athena.webserver.statemachine.StateEntry;
import com.oracle.athena.webserver.statemachine.StateMachine;
import com.oracle.athena.webserver.statemachine.StateQueueResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class ContentReadPipelineMgr extends ConnectionPipelineMgr {

    private static final Logger LOG = LoggerFactory.getLogger(ContentReadPipelineMgr.class);

    private final WebServerConnState connectionState;

    private boolean initialStage;

    private Function<WebServerConnState, StateQueueResult> contentReadSetup = wsConn -> {
        wsConn.determineNextContentRead();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };
    private Function<WebServerConnState, StateQueueResult> contentReadRequestBuffers = wsConn -> {
        if (wsConn.allocContentReadBuffers() == 0) {
            return StateQueueResult.STATE_RESULT_WAIT;
        } else {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
    };

    private Function<WebServerConnState, StateQueueResult> contentReadIntoBuffers = wsConn -> {
        wsConn.readIntoDataBuffers();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerConnState, StateQueueResult> contentReadCheckSlowChannel = wsConn -> {
        if (wsConn.checkSlowClientChannel()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        } else {
            return StateQueueResult.STATE_RESULT_WAIT;
        }
    };

    private Function<WebServerConnState, StateQueueResult> contentReadSendXferResponse = wsConn -> {
        wsConn.sendResponse(wsConn.getHttpParseStatus());
        return StateQueueResult.STATE_RESULT_WAIT;
    };

    private Function<WebServerConnState, StateQueueResult> contentReadConnFinished = wsConn -> {
        wsConn.releaseContentBuffers();
        initialStage = true;
        wsConn.reset();
        return StateQueueResult.STATE_RESULT_FREE;
    };

    private Function<WebServerConnState, StateQueueResult> contentReadSetNextPipeline = wsConn -> {
        initialStage = true;
        wsConn.resetRequestedDataBuffers();
        wsConn.resetBuffersWaiting();
        wsConn.resetDataBufferReadsCompleted();
        wsConn.resetResponses();
        return StateQueueResult.STATE_RESULT_COMPLETE;
    };

    private Function<WebServerConnState, StateQueueResult> contentReadProcessReadError = wsConn -> {
        if (wsConn.processReadErrorQueue()) {
            return StateQueueResult.STATE_RESULT_CONTINUE;
        }
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<WebServerConnState, StateQueueResult> contentReadProcessFinalResponseSend = wsConn -> {
        wsConn.processResponseWriteDone();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerConnState, StateQueueResult> contentReadReleaseBuffers = wsConn -> {
        wsConn.releaseContentBuffers();
        return StateQueueResult.STATE_RESULT_REQUEUE;
    };

    private Function<WebServerConnState, StateQueueResult> contentCalculateMd5 = wsConn -> {
        wsConn.sendBuffersToMd5Worker();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerConnState, StateQueueResult> contentMd5Complete = wsConn -> {
        wsConn.md5CalculateComplete();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    private Function<WebServerConnState, StateQueueResult> contentMd5BufferDone = wsConn -> {
        wsConn.md5BufferWorkComplete();
        return StateQueueResult.STATE_RESULT_CONTINUE;
    };

    ContentReadPipelineMgr(WebServerConnState connState) {
        super(connState, new StateMachine<>());
        this.connectionState = connState;

        initialStage = true;

        /*
         ** Reset the state of the pipeline
         */
        connectionState.resetRequestedDataBuffers();
        connectionState.resetBuffersWaiting();
        connectionState.resetDataBufferReadsCompleted();

        connectionState.resetResponses();
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SETUP_CONTENT_READ, new StateEntry<>(contentReadSetup));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.ALLOC_CONTENT_DATA_BUFFER, new StateEntry<>(contentReadRequestBuffers));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.READ_CONTENT_DATA, new StateEntry<>(contentReadIntoBuffers));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CHECK_SLOW_CHANNEL, new StateEntry<>(contentReadCheckSlowChannel));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SEND_FINAL_RESPONSE, new StateEntry<>(contentReadSendXferResponse));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.CONN_FINISHED, new StateEntry<>(contentReadConnFinished));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.SETUP_NEXT_PIPELINE, new StateEntry<>(contentReadSetNextPipeline));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_READ_ERROR, new StateEntry<>(contentReadProcessReadError));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.PROCESS_FINAL_RESPONSE_SEND, new StateEntry<>(contentReadProcessFinalResponseSend));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.RELEASE_CONTENT_BUFFERS, new StateEntry<>(contentReadReleaseBuffers));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.MD5_CALCULATE, new StateEntry<>(contentCalculateMd5));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.MD5_CALCULATE_COMPLETE, new StateEntry<>(contentMd5Complete));
        connectionStateMachine.addStateEntry(ConnectionStateEnum.MD5_BUFFER_DONE, new StateEntry<>(contentMd5BufferDone));
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

        if (connectionState.hasMd5CompleteBuffers()) {
            return ConnectionStateEnum.MD5_BUFFER_DONE;
        }

        if (connectionState.getDataBufferReadsCompleted() > 0) {
            return ConnectionStateEnum.MD5_CALCULATE;
        }

        if (connectionState.getDataBufferDigestCompleted()) {
            return ConnectionStateEnum.MD5_CALCULATE_COMPLETE;
        }

        /*
         ** This is where the processing of the content data buffers will start. For now, this just
         **   goes directly to the release content buffers stage, which only releases the buffers back to the free
         **   pool.
         */
        if (connectionState.buffersOnEncryptQueue() > 0) {
            return ConnectionStateEnum.RELEASE_CONTENT_BUFFERS;
        }

        /*
         ** Check if the content has all been read in and then proceed to finishing the processing
         **
         ** TODO: Start adding in the steps to process the content data instead of just sending status
         */
        if (connectionState.hasAllConnectionProcessingCompleted() && !connectionState.hasFinalResponseBeenSent()) {
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
