package com.webutils.objectserver.operations;

import com.webutils.objectserver.common.ListObjectData;
import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ObjectInfo;
import com.webutils.webserver.mysql.ObjectTableMgr;
import com.webutils.webserver.mysql.TenancyTableMgr;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.WriteToClient;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
** This retrieves the information about an Object from the ObjectStorageDb
 */
public class RetrieveObjectInfo implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(RetrieveObjectInfo.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.RETRIEVE_OBJECT_INFO;

    private final ObjectServerRequestContext requestContext;

    private final MemoryManager memoryManager;

    private final ObjectInfo objectInfo;

    private final Operation completeCallback;
    private final Operation errorCallback;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> retrieveObjectInfoOps;

    /*
     ** This is to make the execute() function more manageable
     */
    enum ExecutionState {
        READ_FROM_DATABASE,
        DATABASE_READ_ERROR,
        WAIT_FOR_RESPONSE_SEND,
        EMPTY_STATE
    }

    private ExecutionState currState;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public RetrieveObjectInfo(final ObjectServerRequestContext requestContext, final MemoryManager memoryManager,
                              final ObjectInfo objectInfo, final Operation completeCb, final Operation errorCb) {
        this.requestContext = requestContext;
        this.memoryManager = memoryManager;

        this.objectInfo = objectInfo;

        this.completeCallback = completeCb;
        this.errorCallback = errorCb;

        this.retrieveObjectInfoOps = new HashMap<>();

        this.currState = ExecutionState.READ_FROM_DATABASE;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() {
        return requestContext.getRequestId();
    }

    public BufferManagerPointer initialize() {

        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
    ** This builds an in-memory representation of the information needed to access the data from an object.
     */
    public void execute() {
        switch (currState) {
            case READ_FROM_DATABASE:
                /*
                 ** Obtain the object information. The critical pieces to access the object information are:
                 **    Tenancy -
                 **    Namespace - The "holder" of all of the buckets for a particular user in a region
                 **    Bucket Name - A place to organize objects
                 **    Object Name - The actual object that is wanted.
                 **    Version Id - An object can have multiple versions
                 */
                HttpRequestInfo objectPutInfo = requestContext.getHttpInfo();

                WebServerFlavor flavor = requestContext.getWebServerFlavor();

                TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);
                String tenancyUID = tenancyMgr.getTenancyUID("testCustomer", "Tenancy-12345-abcde");

                List<String> requestedFields = new ArrayList<>(List.of("name", "etag", "version", "md5", "size", "time-created", "tier"));

                ListObjectData listData = new ListObjectData(requestContext, objectPutInfo, requestedFields, tenancyUID);
                listData.execute();

                ObjectTableMgr objectMgr = new ObjectTableMgr(flavor, requestContext);
                if (objectMgr.retrieveObjectInfo(objectPutInfo, objectInfo, tenancyUID) == HttpStatus.OK_200) {
                    int objectId = objectInfo.getObjectId();
                    objectInfo.setEtag(objectMgr.getObjectUID(objectId));

                    requestContext.setObjectId(objectId);

                    completeCallback.event();
                    currState = ExecutionState.EMPTY_STATE;
                    break;
                } else {
                    currState = ExecutionState.DATABASE_READ_ERROR;
                    /*
                    ** Fall through
                     */
                }

            case DATABASE_READ_ERROR:
                currState = ExecutionState.WAIT_FOR_RESPONSE_SEND;
                SendObjectGetResponse sendResponse = setupErrorResponseSend();
                sendResponse.event();
                break;

            case WAIT_FOR_RESPONSE_SEND:
                currState = ExecutionState.EMPTY_STATE;
                errorCallback.event();
                break;

            case EMPTY_STATE:
                break;
        }
    }

    public void complete() {

        LOG.info("RetrieveObjectInfo[" + requestContext.getRequestId() + "] complete");

        /*
        ** The following operations are only setup if there was an error reading from the database
         */
        Operation writeToClient = retrieveObjectInfoOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        if (writeToClient != null) {
            writeToClient.complete();
        }

        Operation sendResponse = retrieveObjectInfoOps.remove(OperationTypeEnum.SEND_OBJECT_GET_RESPONSE);
        if (sendResponse != null) {
            sendResponse.complete();
        }

        /*
         ** Call the complete() method for any operations that this one created.
         */
        Collection<Operation> createdOperations = retrieveObjectInfoOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        retrieveObjectInfoOps.clear();
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
        //LOG.info("RetrieveObjectInfo[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("RetrieveObjectInfo[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("RetrieveObjectInfo[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("RetrieveObjectInfo[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("RetrieveObjectInfo[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
    }

    /*
    ** This sets up the operations required to send the error response back to the client. This is used when the
    **   read for the objects information from the database fails for some reason.
     */
    private SendObjectGetResponse setupErrorResponseSend() {

        /*
         ** In the good path, after the HTTP Response is sent back to the client, then start reading in the chunks
         **   that make up the requested object.
         */
        SendObjectGetResponse sendResponse = new SendObjectGetResponse(requestContext, memoryManager, objectInfo);
        retrieveObjectInfoOps.put(sendResponse.getOperationType(), sendResponse);
        BufferManagerPointer clientWritePtr = sendResponse.initialize();

        IoInterface clientConnection = requestContext.getClientConnection();
        WriteToClient writeToClient = new WriteToClient(requestContext, clientConnection, this, clientWritePtr, null);
        retrieveObjectInfoOps.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        return sendResponse;
    }

}
