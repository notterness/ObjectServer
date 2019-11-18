package com.oracle.athena.webserver.connectionstate;

public class ContentReadPipeline extends ConnectionPipeline {

    private WebServerConnState connectionState;

    private boolean initialStage;

    ContentReadPipeline(WebServerConnState connState) {

        super(connState);

        connectionState = connState;

        initialStage = true;

        /*
         ** Reset the state of the pipeline
         */
        connectionState.requestedDataBuffers = 0;
        connectionState.allocatedDataBuffers = 0;
        connectionState.outstandingDataReadCount.set(0);

        connectionState.dataResponseSent.set(false);
        connectionState.finalResponseSent = false;
        connectionState.finalResponseSendDone.set(false);
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

            return ConnectionStateEnum.SETUP_CONTENT_READ;
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

        /*
        ** Check if the content has all been read in and then proceed to finishing the processing
        **
        ** TODO: Start adding in the steps to process the content data instead of just sending status
         */
        boolean doneReadingContent = connectionState.contentAllRead.get();
        System.out.println("WebServerConnState[" + connectionState.getConnStateId() + "] doneReadingContent: " +
                doneReadingContent + " finalResponseSent: " + connectionState.finalResponseSent);

        if (doneReadingContent) {
            if (!connectionState.finalResponseSent) {
                return ConnectionStateEnum.SEND_XFR_DATA_RESP;
            }
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

        /*
         ** TODO: This is not really the exit point for the state machine, but until the
         **   steps for dealing with user data are added it is.
         */
        if (connectionState.dataResponseSent.get() || connectionState.finalResponseSendDone.get()) {
            return ConnectionStateEnum.CONN_FINISHED;
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }
}
