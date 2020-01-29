package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadStorageServerResponseBuffer implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ReadStorageServerResponseBuffer.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.READ_STORAGE_SERVER_RESPONSE_BUFFER;


    private final RequestContext requestContext;

    private final BufferManager storageServerResponseBufferManager;

    private final BufferManagerPointer meterBufferPtr;

    private IoInterface storageServerConnection;

    private BufferManagerPointer readBufferPointer;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    public ReadStorageServerResponseBuffer(final RequestContext requestContext,
                                           final IoInterface connection, final BufferManager storageServerResponseBufferMgr,
                                           final BufferManagerPointer meterBufferPtr) {

        this.requestContext = requestContext;
        this.storageServerConnection = connection;
        this.storageServerResponseBufferManager = storageServerResponseBufferMgr;
        this.meterBufferPtr = meterBufferPtr;

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

        readBufferPointer = storageServerResponseBufferManager.register(this, meterBufferPtr);

        /*
         ** Register the BufferManager and BufferManagerPointer with the clientConnection to allow
         **   data to be read in.
         */
        storageServerConnection.registerReadBufferManager(storageServerResponseBufferManager, readBufferPointer);

        return readBufferPointer;
    }

    public void event() {
        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {
        if (storageServerResponseBufferManager.peek(readBufferPointer) != null) {
            storageServerConnection.readBufferAvailable();
        } else {
            LOG.info("ReadBuffer no buffers to read into");
        }
    }

    public void complete() {
        /*
         ** Unregister the BufferManager and the BufferManagerPointer with the clientConnection
         */
        storageServerConnection.unregisterReadBufferManager();

        /*
        ** Remove the BufferManagerPointer
         */
        storageServerResponseBufferManager.unregister(readBufferPointer);
        readBufferPointer = null;

        /*
         ** Need to remove the reference to the IoManager
         */
        storageServerConnection = null;
    }

    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when a operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ReadStorageServerResponseBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ReadStorageServerResponseBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ReadStorageServerResponseBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ReadStorageServerResponseBuffer[" + requestContext.getRequestId() + "] markAddToQueue(" +
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
        LOG.warn("ReadStorageServerResponseBuffer[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        readBufferPointer.dumpPointerInfo();
        LOG.info("");
    }

}
