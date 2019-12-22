package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class BufferReadMetering implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(BufferReadMetering.class);

    private final int INITIAL_INTEGRATION_BUFFER_ALLOC = 10;

    /*
    ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.METER_BUFFERS;

    private final RequestContext requestContext;

    /*
    ** This is used to determine how many buffers to allocate in the ClientReadBufferManager up
    **   front
     */
    private final WebServerFlavor webServerFlavor;

    private BufferManager clientReadBufferMgr;
    private BufferManagerPointer bufferMeteringPointer;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
    ** The index of the last ByteBuffer allocated
     */
    private int lastAddIndex;

    public BufferReadMetering(final WebServerFlavor flavor, final RequestContext requestContext) {

        this.webServerFlavor = flavor;
        this.requestContext = requestContext;
        this.clientReadBufferMgr = requestContext.getClientReadBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        lastAddIndex = 0;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public BufferManagerPointer initialize() {
        /*
         ** Obtain the pointer used to meter out buffers to the read operation
         */
        bufferMeteringPointer = this.clientReadBufferMgr.register(this);

        /*
        ** If this is the WebServerFlavor.INTEGRATION_TESTS then allocate a limited
        **   number of ByteBuffer(s) up front and then reset the metering
        **   pointer.
         */
        if (webServerFlavor == WebServerFlavor.INTEGRATION_TESTS) {
            ByteBuffer buffer;

            for (int i = 0; i < INITIAL_INTEGRATION_BUFFER_ALLOC; i++) {
                buffer = ByteBuffer.allocate(1024);

                clientReadBufferMgr.offer(bufferMeteringPointer, buffer);
            }

            /*
            ** Set the pointer back to the beginning of the BufferManager
             */
            lastAddIndex = clientReadBufferMgr.reset(bufferMeteringPointer);
        } else {
            /*
            ** Fill in the entire ClientReadBufferManager and StorageServerWriteBufferManager
            **   with ByteBuffers.
             */
        }

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
        int nextLocation = clientReadBufferMgr.updateProducerWritePointer(bufferMeteringPointer);
        if (nextLocation == lastAddIndex) {
            /*
            ** Need to add another buffer
             */
        }
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
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when an Operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
     **   of which queue the connection is on. It will probably clean up the code some.
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("BufferReadMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (onDelayedQueue) {
            if (!delayedExecutionQueue) {
                LOG.warn("BufferReadMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
            }

            onDelayedQueue = false;
            nextExecuteTime = 0;
        } else if (onExecutionQueue){
            if (delayedExecutionQueue) {
                LOG.warn("BufferReadMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on workQueue");
            }

            onExecutionQueue = false;
        } else {
            LOG.warn("BufferReadMetering[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            nextExecuteTime = System.currentTimeMillis() + TIME_TILL_NEXT_TIMEOUT_CHECK;
            onDelayedQueue = true;
        } else {
            onExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return onDelayedQueue;
    }

    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        //LOG.info("BufferReadMetering[" + requestContext.getRequestId() + "] waitTimeElapsed " + currTime);
        return true;
    }

    /*
    ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        bufferMeteringPointer.dumpPointerInfo();
        LOG.info("");
    }
}
