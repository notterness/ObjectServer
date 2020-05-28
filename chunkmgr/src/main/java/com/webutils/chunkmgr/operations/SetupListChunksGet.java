package com.webutils.chunkmgr.operations;

import com.webutils.chunkmgr.requestcontext.ChunkAllocRequestContext;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SetupListChunksGet implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupListChunksGet.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_LIST_CHUNKS_GET;

    private final ChunkAllocRequestContext requestContext;

    private final MemoryManager memoryManager;

    private final Operation completeCallback;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> HandlerOperations;

    private boolean setupMethodDone;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the ListChunks GET
     **   method (handled as LIST_CHUNKS_METHOD).
     ** The completeCb will call the DetermineRequest operation's event() method when the GET completes.
     */
    public SetupListChunksGet(final ChunkAllocRequestContext requestContext, final MemoryManager memoryManager,
                              final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.completeCallback = completeCb;

        /*
         ** Setup the list of Operations currently used to handle the ListChunks GET
         */
        HandlerOperations = new HashMap<>();

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

        setupMethodDone = false;

        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {
        if (!setupMethodDone) {
            ListChunks listChunks = new ListChunks(requestContext, memoryManager, this);
            HandlerOperations.put(listChunks.getOperationType(), listChunks);
            BufferManagerPointer responseWritePtr = listChunks.initialize();

            WriteToClient writeToClient = new WriteToClient(requestContext, requestContext.getClientConnection(),
                    this, responseWritePtr, null);
            HandlerOperations.put(writeToClient.getOperationType(), writeToClient);
            writeToClient.initialize();

            listChunks.event();

            setupMethodDone = true;
        } else {
            /*
             */
            complete();
        }
    }

    /*
     ** This is called from the WriteToClient operation when it has completed its work.
     */
    public void complete() {
        /*
         ** Call the complete() methods for all of the Operations created to handle the GET method that did not have
         **   ordering dependencies due to the registrations with the BufferManager(s).
         ** Need to tear down the operations in the correct order to insure that the dependencies between the
         **   BufferManagerPointers are torn down correctly
         */
        Operation operation;

        operation = HandlerOperations.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        operation.complete();

        operation = HandlerOperations.remove(OperationTypeEnum.LIST_CHUNKS);
        operation.complete();

        Collection<Operation> createdOperations = HandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        HandlerOperations.clear();

        completeCallback.event();

        LOG.info("SetupListChunksGet[" + requestContext.getRequestId() + "] completed");
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
        //LOG.info("SetupListChunksGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupListChunksGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupListChunksGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupListChunksGet[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupListChunksGet[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = HandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
