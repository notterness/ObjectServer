package com.webutils.objectserver.operations;

import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SetupObjectPut implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupObjectPut.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_OBJECT_PUT;

    private final ObjectServerRequestContext requestContext;

    private final MemoryManager memoryManager;

    private final Operation metering;

    private final Operation completeCallback;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
    ** There are two operations required to read data out of the clientReadBufferMgr and process it
    **   The Md5 Digest and the Encryption operations.
    **
    ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> putHandlerOperations;

    private boolean setupMethodDone;

    /*
    ** This is used to setup the initial Operation dependencies required to handle the Object PUT
    **   request. This will setup the write of the Object information to the database and once that completes, it will
    **   signal the ObjectPut_P2 operation to execute. This insures that no real work starts until the Object information
    **   is safely stored.
    **
    ** The completeCb will call the DetermineRequest operation's event() method when the Object PUT completes.
    **   Currently, the Object PUT is marked complete when all the PUT object data is written to the Storage Servers
    **   and the Md5 Digest is computed and the comparison against the expected result done.
     */
    public SetupObjectPut(final ObjectServerRequestContext requestContext, final MemoryManager memoryManager, final Operation metering,
                          final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.metering = metering;
        this.completeCallback = completeCb;

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        putHandlerOperations = new HashMap<>();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        setupMethodDone = false;
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
        if (!setupMethodDone) {
            /*
            ** The ObjectPut_P2 operation is run after the CreateObject operation has updated the database with all
            **   the information about the object to be saved. It provides the gate where the wait for the Md5 digest
            **   calculation and waiting for all the data to be written to the Storage Servers takes place.
             */
            ObjectPut_P2 objectPutHandler = new ObjectPut_P2(requestContext, memoryManager, metering, this);
            objectPutHandler.initialize();

            /*
            ** Setup the operation to write the Object information to the database. This needs to complete prior to
            **   the data being written to the Storage Servers. The CreateObject operation will event() the
            **   ObjectPut_P2 operation when it completes successfully. If there was an error creating the object
            **   in the database, it will event() this (SetupObjectPut) to allow the error response to be sent and
            **   cleanup to take place.
            **
            ** NOTE: At this point in the code development, the CreateObject operation does not have any code
            **   that runs in the complete() method. So , no need to call it during the cleanup below.
             */
            CreateObject createObject = new CreateObject(requestContext, objectPutHandler, this);
            putHandlerOperations.put(createObject.getOperationType(), createObject);
            createObject.initialize();
            createObject.event();

            setupMethodDone = true;
        } else {
            complete();
        }
    }

    /*
    ** This is called from the ObjectPut_P2 operation when it has completed its work or an error occurred.
     */
    public void complete() {
        LOG.info("SetupObjectPut[" + requestContext.getRequestId() + "] completed");
        completeCallback.event();

        putHandlerOperations.clear();
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
        //LOG.info("SetupObjectPut[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupObjectPut[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupObjectPut[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupObjectPut[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupObjectPut[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = putHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
