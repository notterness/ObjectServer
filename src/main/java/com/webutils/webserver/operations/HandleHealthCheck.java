package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleHealthCheck implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(HandleHealthCheck.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.HEALTH_CHECK;

    /*
     ** The operations are all tied together via the RequestContext
     */
    private final RequestContext requestContext;

    /*
     ** The completeCallback will cause the final response to be sent out.
     */
    private final Operation completeCallback;

    /*
     ** The states required to complete the Healtyh Check operation.
     **   1) Check the health of the service
     **   2) Call back the "Determine Request" handler
     */
    private enum ExecutionState {
        CHECK_HEALTH,
        CALLBACK_OPS,
        EMPTY_STATE
    }

    private ExecutionState currState;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the Health Check GET
     **   request. This is how the health of the various services is monitored.
     */
    public HandleHealthCheck(final RequestContext requestContext, final Operation completeCb) {

        this.requestContext = requestContext;
        this.completeCallback = completeCb;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         **
         */
        currState = ExecutionState.CHECK_HEALTH;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
     ** This returns the BufferManagerPointer obtained by this operation, if there is one. If this operation
     **   does not use a BufferManagerPointer, it will return null.
     */
    public BufferManagerPointer initialize() {

        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {
        switch (currState) {
            case CHECK_HEALTH:
                if (requestContext.getHttpInfo().getServiceDisable()) {
                    /*
                    ** Mark the service as disabled
                     */
                } else if (requestContext.getHttpInfo().getServiceEnable()) {
                    /*
                    ** Mark the service as enabled
                     */
                }

                requestContext.getHttpInfo().setResponseHeaders(buildSuccessHeader());
                currState = ExecutionState.CALLBACK_OPS;
                /*
                 ** Fall through
                 */

            case CALLBACK_OPS:
                completeCallback.event();
                currState = ExecutionState.EMPTY_STATE;
                break;

            case EMPTY_STATE:
                break;
        }
    }

    /*
     ** This complete() is called when the ReadObjectChunks operation has read all the chunks in and transferred the
     **   data to the client.
     */
    public void complete() {
        LOG.info("HandleHealthCheck[" + requestContext.getRequestId() + "] complete()");
    }


    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when an Operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("HandleHealthCheck[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("HandleHealthCheck[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("HandleHealthCheck[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("HandleHealthCheck[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
        } else {
            onExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return false;
    }

    public boolean hasWaitTimeElapsed() {
        LOG.warn("HandleHealthCheck[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
    }

    /*
     ** This builds the OK_200 response headers for the HealthCheck method (GET). This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     */
    private String buildSuccessHeader() {
        String successHeader;

        HttpRequestInfo requestInfo = requestContext.getHttpInfo();
        String opcClientRequestId = requestInfo.getOpcClientRequestId();
        int opcRequestId = requestInfo.getRequestId();

        if (opcClientRequestId != null) {
            successHeader = HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId + "\n" +
                    HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\n";
        } else {
            successHeader = HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\n";
        }

        return successHeader;
    }

}
