package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class BufferWriteMetering implements Operation {


    private static final Logger LOG = LoggerFactory.getLogger(BufferWriteMetering.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.BUFFER_WRITE_METERING;

    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    private final BufferManager clientWriteBufferMgr;
    private BufferManagerPointer bufferMeteringPointer;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** The BufferReadMetering operation will populate the clientWriteBufferManager with the
     **   ByteBuffer(s) and will set the allocation BufferManagerPointer back to the start of
     **   the BufferManager (meaning that no ByteBuffer(s) are available to write data into).
     */
    public BufferWriteMetering(final RequestContext requestContext, final MemoryManager memoryManager) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;

        this.clientWriteBufferMgr = requestContext.getClientWriteBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         ** Obtain the pointer used to meter out buffers to the read operation
         */
        bufferMeteringPointer = this.clientWriteBufferMgr.register(this);

        /*
         ** If this is the WebServerFlavor.INTEGRATION_TESTS then allocate a limited
         **   number of ByteBuffer(s) up front and then reset the metering
         **   pointer.
         */
        int buffersToAllocate = memoryManager.getBufferManagerRingSize();

        for (int i = 0; i < buffersToAllocate; i++) {
            ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, clientWriteBufferMgr, operationType);

            clientWriteBufferMgr.offer(bufferMeteringPointer, buffer);
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
        clientWriteBufferMgr.reset(bufferMeteringPointer);

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
        clientWriteBufferMgr.updateProducerWritePointer(bufferMeteringPointer);
    }

    public void complete() {

        LOG.info("BufferWriteMetering complete()");

        /*
         ** Set the pointer back to the beginning of the BufferManager to release the allocated memory
         */
        clientWriteBufferMgr.reset(bufferMeteringPointer);

        int buffersAllocated = memoryManager.getBufferManagerRingSize();

        for (int i = 0; i < buffersAllocated; i++) {
            ByteBuffer buffer = clientWriteBufferMgr.getAndRemove(bufferMeteringPointer);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer, clientWriteBufferMgr);
            } else {
                LOG.warn("BufferWriteMetering[" + requestContext.getRequestId() + "] null buffer i: " + i);
            }
        }

        /*
         ** Release the metering pointer
         */
        clientWriteBufferMgr.unregister(bufferMeteringPointer);
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
        //LOG.info("BufferWriteMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("BufferWriteMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("BufferWriteMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
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
