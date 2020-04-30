package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloseOutRequest implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(CloseOutRequest.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.CLOSE_OUT_REQUEST;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    private BufferManagerPointer writeStatusPtr;

    public CloseOutRequest(final RequestContext requestContext) {

        this.requestContext = requestContext;

        /*
        ** This starts out not being on any queue
         */
        onExecutionQueue = false;
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

    /*
     **
     */
    public void execute() {
        /*
        ** Close out the connection and place the RequestContext back on the "free" list
         */
        requestContext.cleanupServerRequest();
    }

    /*
    ** This will never be called for the CloseOutRequest. When the execute() method completes, the
    **   RequestContext is no longer "running".
     */
    public void complete() {

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
        //LOG.info("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markAddToQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
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
        LOG.warn("CloseOutRequest[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info("  " + level + ":   requestId[" + requestContext.getRequestId() + "] type: " + operationType);
    }

}
