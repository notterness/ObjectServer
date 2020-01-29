package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class BufferReadMetering implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(BufferReadMetering.class);

    /*
    ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.METER_READ_BUFFERS;

    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    private BufferManager clientReadBufferMgr;
    private BufferManagerPointer bufferMeteringPointer;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
    ** The BufferReadMetering operation will populate the clientReadBufferManager with the
    **   ByteBuffer(s) and will set the allocation BufferManagerPointer back to the start of
    **   the BufferManager (meaning that no ByteBuffer(s) are available to read data into).
     */
    public BufferReadMetering(final RequestContext requestContext, final MemoryManager memoryManager) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;

        this.clientReadBufferMgr = requestContext.getClientReadBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         ** Obtain the pointer used to meter out buffers to the read operation
         */
        bufferMeteringPointer = this.clientReadBufferMgr.register(this);

        /*
         ** If this is the WebServerFlavor.INTEGRATION_TESTS then allocate a limited
         **   number of ByteBuffer(s) up front and then reset the metering
         **   pointer.
         */
        int buffersToAllocate = requestContext.getBufferManagerRingSize();

        for (int i = 0; i < buffersToAllocate; i++) {
            ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, clientReadBufferMgr);

            clientReadBufferMgr.offer(bufferMeteringPointer, buffer);
        }
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
        clientReadBufferMgr.reset(bufferMeteringPointer);

        return bufferMeteringPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
    ** For the metering, this is where the pointer for buffers that are available are metered out according to
    **   customer requirements and limitations. The buffers can be preallocated on the BufferManager and only
    **   passed out as needed or as requested.
     */
    public void execute() {

        /*
        ** Add a free buffer to the ClientReadBufferManager
         */
        clientReadBufferMgr.updateProducerWritePointer(bufferMeteringPointer);
    }

    public void complete() {

        /*
         ** Set the pointer back to the beginning of the BufferManager to release the allocated memory
         */
        clientReadBufferMgr.reset(bufferMeteringPointer);

        int buffersAllocated = requestContext.getBufferManagerRingSize();

        for (int i = 0; i < buffersAllocated; i++) {
            ByteBuffer buffer = clientReadBufferMgr.getAndRemove(bufferMeteringPointer);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer, clientReadBufferMgr);
            } else {
                LOG.warn("BufferReadMetering[" + requestContext.getRequestId() + "] null buffer i: " + i);
            }
        }

        /*
        ** Release the metering pointer
         */
        clientReadBufferMgr.unregister(bufferMeteringPointer);
        bufferMeteringPointer = null;
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
        //LOG.info("BufferReadMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("BufferReadMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("BufferReadMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
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
        if (bufferMeteringPointer != null) {
            /*
             ** To handle the case after complete() has been called
             */
            bufferMeteringPointer.dumpPointerInfo();
        }
        LOG.info("");
    }
}
