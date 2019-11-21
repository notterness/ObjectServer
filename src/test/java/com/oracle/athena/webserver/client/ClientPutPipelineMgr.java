package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.connectionstate.ConnectionPipelineMgr;
import com.oracle.athena.webserver.connectionstate.ConnectionStateEnum;
import com.oracle.athena.webserver.statemachine.StateQueueResult;

public class ClientPutPipelineMgr extends ConnectionPipelineMgr {

    private ClientConnState connectionState;

    private boolean initialStage;

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

        return result;
    }
}
