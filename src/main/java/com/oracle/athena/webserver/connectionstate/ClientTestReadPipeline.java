package com.oracle.athena.webserver.connectionstate;

public class ClientTestReadPipeline extends ConnectionPipeline {

    private ClientConnState connectionState;

    private boolean initialStage;

    ClientTestReadPipeline(ClientConnState connState) {

        super(connState);

        connectionState = connState;

        initialStage = true;

        connectionState.bufferAllocationFailed.set(false);

        connectionState.requestedDataBuffers = 0;
        connectionState.allocatedDataBuffers = 0;
        connectionState.outstandingDataReadCount.set(0);
        connectionState.dataBufferReadsCompleted.set(0);

        connectionState.contentAllRead.set(false);

        connectionState.clientCallbackCompleted = false;
    }

    /*
     ** This determines the pipeline stages used to read in the content data.
     */
    ConnectionStateEnum nextPipelineStage() {

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
        boolean outOfMemory = connectionState.bufferAllocationFailed.get();
        if (!outOfMemory) {
            if (connectionState.requestedDataBuffers > 0) {
                return ConnectionStateEnum.ALLOC_CLIENT_DATA_BUFFER;
            }
        }

        /*
         ** The NIO.2 AsynchronousChannelRead can only have a single outstanding read at at time.
         **
         ** TODO: Support the NIO.2 read that can be passed in an array of ByteBuffers
         */
        if ((connectionState.allocatedDataBuffers > 0)  && (connectionState.outstandingDataReadCount.get() == 0)){
            return ConnectionStateEnum.READ_CLIENT_DATA;
        }

        int dataReadComp = connectionState.dataBufferReadsCompleted.get();
        if (dataReadComp > 0) {
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
        if (connectionState.clientCallbackCompleted == true) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        /*
         ** Check if the content has all been read in and then proceed to finishing the processing
         **
         ** TODO: Start adding in the steps to process the content data instead of just sending status
         */
        boolean doneReadingContent = connectionState.contentAllRead.get();
        if (doneReadingContent) {
            return ConnectionStateEnum.CLIENT_READ_CB;
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **    reads.
         */
        if (connectionState.channelError.get()) {
            int dataReadsPending = connectionState.outstandingDataReadCount.get();

            if (dataReadsPending == 0) {
                return ConnectionStateEnum.CONN_FINISHED;
            }
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }

}
