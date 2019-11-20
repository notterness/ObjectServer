package com.oracle.athena.webserver.connectionstate;

public class OutOfResourcePipelineMgr extends ConnectionPipelineMgr {
    private WebServerConnState connectionState;

    private boolean initialStage;

    OutOfResourcePipelineMgr(WebServerConnState connState) {

        super(connState);

        this.connectionState = connState;

        initialStage = true;

        /*
         ** Reset the state of the pipeline
         */
        connectionState.resetResponses();
    }

    /*
     ** This determines the pipeline stages used to read in the content data.
     */
    public ConnectionStateEnum nextPipelineStage() {

        System.out.println("OutOfResourcePipelineMgr[" + connectionState.getConnStateId() + "] initialState: " + initialStage);

        /*
         ** First setup to perform the content reads. This is required since the buffer used to read in the
         **   HTTP headers may have also had data for the content at the end of it.
         */
        if (initialStage) {
            initialStage = false;

            return ConnectionStateEnum.SEND_OUT_OF_RESOURCE_RESP;
        }

        /*
         ** Check if there was a channel error and cleanup if there was as there is not point waiting for
         **   the write to complete
         */
        if (connectionState.hasChannelFailed()) {
            return ConnectionStateEnum.CONN_FINISHED;
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