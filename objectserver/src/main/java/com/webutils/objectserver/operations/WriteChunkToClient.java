package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteChunkToClient implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(WriteChunkToClient.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.WRITE_CHUNK_TO_CLIENT;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    private IoInterface clientConnection;

    /*
     ** The following is the operation to run when all of the data in the chunk has been written to the client
     */
    private final SetupChunkRead finalOperationToRun;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    private final BufferManager decryptBufferManager;
    private final BufferManagerPointer decryptedPointer;
    private BufferManagerPointer writePointer;
    private BufferManagerPointer writeDonePointer;

    public WriteChunkToClient(final RequestContext requestContext, final IoInterface connection,
                              final SetupChunkRead opCompCb, final BufferManager decryptBufferMgr,
                              final BufferManagerPointer decryptedPtr) {

        this.requestContext = requestContext;
        this.clientConnection = connection;
        this.finalOperationToRun = opCompCb;

        this.decryptBufferManager = decryptBufferMgr;
        this.decryptedPointer = decryptedPtr;

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
     */
    public BufferManagerPointer initialize() {
        LOG.info("WriteChunkToClient initialize()");

        /*
         ** The writePointer BufferManagerPointer is used to event() this operation when
         **   data is placed where the bufferInfillPointer is pointing and then the
         **   bufferInfillPointer is updated (incremented).
         */
        writePointer = decryptBufferManager.register(this, decryptedPointer);

        writeDonePointer = decryptBufferManager.register(this, writePointer);

        /*
         ** Register with the IoInterface to perform writes to the client
         */
        clientConnection.registerWriteBufferManager(decryptBufferManager, writePointer);

        return writePointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     */
    public void execute() {
        /*
         ** The following is to check if there is data ready to be written to the client.
         */
        if (decryptBufferManager.peek(writePointer) != null) {
            clientConnection.writeBufferReady();
        }

        /*
         ** The following is to check if data has been written to the client (meaning there
         **   are buffers available that are dependent upon the writePointer).
         */
        if (decryptBufferManager.poll(writeDonePointer) != null) {
            /*
             ** Since there are "buffers" available, it means the data was written out the SocketChannel
             **   and from this server's perspective it is done IF all of the buffers have been filled.
             ** Done with this client connection as well since this is only being used to write the HTTP Response
             */
            finalOperationToRun.setAllDataWritten();
            finalOperationToRun.event();
        }

    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {
        clientConnection.unregisterWriteBufferManager();
        clientConnection = null;

        decryptBufferManager.unregister(writeDonePointer);
        writeDonePointer = null;

        decryptBufferManager.unregister(writePointer);
        writePointer = null;
    }

    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when an operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("WriteChunkToClient[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("WriteChunkToClient[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("WriteChunkToClient[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("WriteChunkToClient[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("WriteToClient[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("      No BufferManagerPointers");
        LOG.info("");
    }

}
