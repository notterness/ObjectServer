package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferReadMetering implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(BufferReadMetering.class);

    /*
    ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.METER_BUFFERS;

    private final RequestContext requestContext;

    private BufferManager clientReadBufferMgr;
    private BufferManagerPointer bufferMeteringPointer;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (connOnDelayedQueue) or the normal
     **   execution queue (connOnExecutionQueue) is will always go on the execution queue.
     */
    private boolean connOnDelayedQueue;
    private boolean connOnExecutionQueue;
    private long nextExecuteTime;

    public BufferReadMetering(final RequestContext requestContext) {

        this.requestContext = requestContext;
        this.clientReadBufferMgr = requestContext.getClientReadBufferManager();

        /*
         ** Obtain the pointer used to meter out buffers to the read operation
         */
        this.bufferMeteringPointer = this.clientReadBufferMgr.register(this);

        /*
         ** This starts out not being on any queue
         */
        connOnDelayedQueue = false;
        connOnExecutionQueue = false;
        nextExecuteTime = 0;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public BufferManagerPointer initialize() {

        return bufferMeteringPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {

    }

    public void complete() {
        /*
        ** Release the metering pointer
         */
        clientReadBufferMgr.unregister(bufferMeteringPointer);
    }

    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the ServerWorkerThread to update the queue
     **     the connection is on when the connection is removed from the queue.
     **   markAddedToQueue - This method is used when a connection is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **
     ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
     **   of which queue the connection is on. It will probably clean up the code some.
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ConnectionState[" + connStateId + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (connOnDelayedQueue) {
            if (!delayedExecutionQueue) {
                LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
            }

            connOnDelayedQueue = false;
            nextExecuteTime = 0;
        } else if (connOnExecutionQueue){
            if (delayedExecutionQueue) {
                LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on workQueue");
            }

            connOnExecutionQueue = false;
        } else {
            LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            nextExecuteTime = System.currentTimeMillis() + TIME_TILL_NEXT_TIMEOUT_CHECK;
            connOnDelayedQueue = true;
        } else {
            connOnExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return connOnExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return connOnDelayedQueue;
    }

    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        //LOG.info("CloseOutRequest[" + requestContext.getRequestId() + "] waitTimeElapsed " + currTime);
        return true;
    }

}
