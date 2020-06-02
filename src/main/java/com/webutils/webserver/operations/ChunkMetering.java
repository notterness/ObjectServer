package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ChunkMetering implements Operation {


    private static final Logger LOG = LoggerFactory.getLogger(ChunkMetering.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.CHUNK_METER_BUFFERS;

    private final RequestContext requestContext;

    private final BufferManager chunkBufferManager;
    private final BufferManagerPointer meteringPointer;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     */
    public ChunkMetering(final RequestContext requestContext, final BufferManager chunkBufferMgr,
                         final BufferManagerPointer meteringPtr) {

        this.requestContext = requestContext;

        this.chunkBufferManager = chunkBufferMgr;
        this.meteringPointer = meteringPtr;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    public BufferManagerPointer initialize() {

        /*
         ** Set the pointer back to the beginning of the BufferManager. The BufferReadMetering operation will need
         **   to have its execute() method called to dole out ByteBuffer(s) to perform read operations into.
         */
        chunkBufferManager.reset(meteringPointer);

        return meteringPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     ** For the metering, this is where the pointer for buffers that are available are metered out according to
     **   customer requirements and limitations. The buffers can be pre-allocated on the BufferManager and only
     **   passed out as needed or as requested.
     */
    public void execute() {

        /*
         ** Add a free buffer to the ClientReadBufferManager
         */
        chunkBufferManager.updateProducerWritePointer(meteringPointer);
    }

    public void complete() {

        /*
         ** Set the pointer back to the beginning of the BufferManager
         */
        chunkBufferManager.reset(meteringPointer);
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
        //LOG.info("ChunkMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ChunkMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ChunkMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        onExecutionQueue = true;
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return false;
    }

    public boolean hasWaitTimeElapsed() {
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
    }
}
