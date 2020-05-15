package com.webutils.objectserver.operations;

import com.webutils.objectserver.common.ListObjectData;
import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.TenancyTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SetupObjectList implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupObjectList.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.SETUP_OBJECT_LIST;


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
    private boolean operationSetupDone;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the Storage Server GET
     **   request. This uses the GET command, but the "/o" field is empty to indicate the it is a request for all the
     **   objects that are kept within a bucket.
     */
    public SetupObjectList(final ObjectServerRequestContext requestContext, final MemoryManager memoryManager,
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
        operationSetupDone = false;
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
        if (!operationSetupDone) {
            HttpRequestInfo objectListInfo = requestContext.getHttpInfo();

            TenancyTableMgr tenancyMgr = new TenancyTableMgr(requestContext.getWebServerFlavor());
            String tenancyUID = tenancyMgr.getTenancyUID("testCustomer", "Tenancy-12345-abcde");

            List<String> requestedFields = new ArrayList<>(List.of("name", "etag", "version", "md5", "size", "time-created", "tier"));
            ListObjectData listData = new ListObjectData(requestContext, objectListInfo, requestedFields, tenancyUID);
            listData.execute();

            event();

            operationSetupDone = true;
        } else {
            complete();
        }
    }

    /*
     ** This complete() is called when the ReadObjectChunks operation has read all the chunks in and transferred the
     **   data to the client.
     */
    public void complete() {

        LOG.info("SetupObjectList[" + requestContext.getRequestId() + "] complete()");

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
        //LOG.info("SetupObjectList[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupObjectList[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupObjectList[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupObjectList[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupObjectList[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
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
