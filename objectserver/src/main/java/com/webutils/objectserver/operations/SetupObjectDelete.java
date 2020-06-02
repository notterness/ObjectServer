package com.webutils.objectserver.operations;

import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ObjectInfo;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SetupObjectDelete implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(SetupObjectDelete.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.SETUP_OBJECT_DELETE;


    /*
     ** The operations are all tied together via the RequestContext
     */
    private final ObjectServerRequestContext requestContext;

    private final MemoryManager memoryManager;

    /*
     ** The completeCallback will cause the final response to be sent out.
     */
    private final Operation completeCallback;

    /*
     **
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> objectGetHandlerOps;

    /*
     ** This is used to prevent the Operation setup code from being called multiple times in the execute() method
     */
    private boolean deleteOperationSetupDone;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the Storage Server DELETE
     **   request. This is how are deleting from the Object Server and their chunks on the Storage Server are
     **   freed up.
     */
    public SetupObjectDelete(final ObjectServerRequestContext requestContext, final MemoryManager memoryManager,
                          final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.completeCallback = completeCb;

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        this.objectGetHandlerOps = new HashMap<>();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         **
         */
        deleteOperationSetupDone = false;
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
        if (!deleteOperationSetupDone) {
            HttpRequestInfo objectHttpInfo = requestContext.getHttpInfo();

            /*
             ** The ObjectInfo is used to hold the information required to read the object data from the Storage Servers.
             */
            ObjectInfo objectInfo = new ObjectInfo(objectHttpInfo);

            /*
             ** When all the data is read in from the database (assuming it was successful), then send the HTTP Response
             **   back to the client.
             */
            DeleteObjectInfo deleteObjectInfo = new DeleteObjectInfo(requestContext, memoryManager, objectInfo,this);
            objectGetHandlerOps.put(deleteObjectInfo.getOperationType(), deleteObjectInfo);
            deleteObjectInfo.initialize();
            deleteObjectInfo.event();

            deleteOperationSetupDone = true;
        } else {
            complete();
        }
    }

    /*
     ** This complete() is called when the ReadObjectChunks operation has read all the chunks in and transferred the
     **   data to the client.
     */
    public void complete() {

        LOG.info("SetupObjectDelete[" + requestContext.getRequestId() + "] complete()");

        /*
         ** Call the complete() method for any operations that this one created.
         */
        Collection<Operation> createdOperations = objectGetHandlerOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        objectGetHandlerOps.clear();

        completeCallback.event();
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
        //LOG.info("SetupObjectDelete[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupObjectDelete[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupObjectDelete[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupObjectDelete[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupObjectDelete[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = objectGetHandlerOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
