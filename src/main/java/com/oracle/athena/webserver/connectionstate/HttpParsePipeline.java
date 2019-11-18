package com.oracle.athena.webserver.connectionstate;


public class HttpParsePipeline extends ConnectionPipeline {

    private WebServerConnState connectionState;

    private boolean initialStage;

    HttpParsePipeline(WebServerConnState connState) {

        super(connState);

        connectionState = connState;

        initialStage = true;

        /*
        ** Reset the state of the pipeline
         */
        connectionState.requestedHttpBuffers = 0;
        connectionState.allocatedHttpBufferCount = 0;

        connectionState.outstandingHttpReadCount.set(0);
        connectionState.httpBufferReadsCompleted.set(0);
        connectionState.httpHeaderParsed.set(false);

        /*
        ** This must be set to false here as there may be content data included in a buffer used to
        **   to read in the HTTP headers.
         */
        connectionState.contentAllRead.set(false);
    }

    /*
    ** This determines the pipeline stages used to read in and parse the HTTP headers.
     */
    ConnectionStateEnum nextPipelineStage() {

        /*
        ** Perform the initial setup
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.INITIAL_SETUP;
        }

        /*
         ** Check if the header parsing is completed and if so, setup the initial reads
         **   for the content.
         **
         ** NOTE: This check is done prior to seeing if buffers need to be allocated to
         **   read in content data.
         */
        boolean headerParsed = connectionState.httpHeaderParsed.get();
        if (headerParsed) {
            /*
             ** Figure out how many buffers to read.
             */
            return ConnectionStateEnum.SETUP_NEXT_PIPELINE;
        }

        /*
         ** Are there outstanding buffers to be allocated. If the code had attempted to allocate
         **   buffers and failed, check if there is other work to do. No point trying the buffer
         **   allocation right away.
         **
         */
        boolean outOfMemory = connectionState.bufferAllocationFailed.get();
        if (!outOfMemory) {
            if (connectionState.requestedHttpBuffers > 0) {
                return ConnectionStateEnum.ALLOC_HTTP_BUFFER;
            }
        }

        /*
         ** Are there buffers waiting to have a read performed?
         */
        if (connectionState.allocatedHttpBufferCount > 0) {
            return ConnectionStateEnum.READ_HTTP_BUFFER;
        }

        /*
         ** Are there completed reads, priority is processing the HTTP header
         */
        int httpReadComp = connectionState.httpBufferReadsCompleted.get();
        if (httpReadComp > 0) {
            return ConnectionStateEnum.PARSE_HTTP_BUFFER;
        }

        /*
         ** Check if there was a channel error and cleanup if there are no outstanding
         **    reads.
         */
        if (connectionState.channelError.get()) {
            int httpReadsPending = connectionState.outstandingHttpReadCount.get();

            if (httpReadsPending == 0) {
                return ConnectionStateEnum.CONN_FINISHED;
            }
        }

        return ConnectionStateEnum.CHECK_SLOW_CHANNEL;
    }
}
