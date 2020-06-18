package com.webutils.objectserver.operations;

import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.BucketTableMgr;
import com.webutils.webserver.mysql.NamespaceTableMgr;
import com.webutils.webserver.mysql.ObjectTableMgr;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.WriteToClient;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SetupBucketDelete implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(SetupBucketDelete.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.SETUP_BUCKET_DELETE;


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
     ** The states required to complete the delete bucket operation.
     **   1) Obtain the bucket to be deleted
     **   2) First insure that all the objects within the bucket have been deleted.
     **   3) Assuming there are no objects, then delete the bucket
     */
    private enum ExecutionState {
        OBTAIN_BUCKET,
        CHECK_FOR_OBJECTS,
        DELETE_BUCKET,
        SEND_STATUS,
        CALLBACK_OPS,
        EMPTY_STATE
    }

    private ExecutionState currState;

    private final BucketTableMgr bucketMgr;
    private int bucketId;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> deleteBucketInfoOps;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the Bucket DELETE
     **   request. This is how Buckets are deleted from the Object Server.
     */
    public SetupBucketDelete(final ObjectServerRequestContext requestContext, final MemoryManager memoryManager,
                             final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.completeCallback = completeCb;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        this.deleteBucketInfoOps = new HashMap<>();

        /*
         **
         */
        currState = ExecutionState.OBTAIN_BUCKET;

        bucketMgr = new BucketTableMgr(requestContext.getWebServerFlavor(), requestContext.getRequestId(),
                requestContext.getHttpInfo());
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
        WebServerFlavor flavor = requestContext.getWebServerFlavor();

        switch (currState) {
            case OBTAIN_BUCKET:
                NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
                String namespace = requestContext.getHttpInfo().getNamespace();

                String namespaceUID = namespaceMgr.getNamespaceUID(namespace, requestContext.getTenancyId());
                if (namespaceUID == null) {
                    LOG.warn("DeleteBucket failed - unknown namespace: " + namespace);
                    String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.BAD_REQUEST_400 + "\"" +
                            "\r\n  \"message\": \"Unknown namespace - " + namespace + "\"" +
                            "\r\n}";
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);

                    currState = ExecutionState.SEND_STATUS;
                    event();
                    break;
                }

                String bucketName = requestContext.getHttpInfo().getBucket();
                bucketId = bucketMgr.getBucketId(bucketName, namespaceUID);
                if (bucketId == -1) {
                    LOG.warn("DeleteBucket failed - unknown bucket: " + bucketName);
                    String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.BAD_REQUEST_400 + "\"" +
                            "\r\n  \"message\": \"Unknown bucket - " + bucketName + "\"" +
                            "\r\n}";
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);

                    currState = ExecutionState.SEND_STATUS;
                    event();
                    break;
                }
                currState = ExecutionState.CHECK_FOR_OBJECTS;
                /*
                ** Fall through
                 */

            case CHECK_FOR_OBJECTS:
                ObjectTableMgr objectMgr = new ObjectTableMgr(flavor, requestContext);
                int objectCount = objectMgr.getObjectCount(bucketId);
                if (objectCount != 0) {
                    LOG.warn("DeleteBucket failed - not empty object count: " + objectCount);
                    String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.CONFLICT_409 + "\"" +
                            "\r\n  \"message\": \"Bucket must be empty prior to deletion\"" +
                            "\r\n  \"bucketName\": \"" + requestContext.getHttpInfo().getBucket() + "\"" +
                            "\r\n  \"objectCount\": " + objectCount +
                            "\r\n}";
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.CONFLICT_409, failureMessage);

                    currState = ExecutionState.SEND_STATUS;
                    event();
                    break;
                }
                currState = ExecutionState.DELETE_BUCKET;
                /*
                ** Fall through
                 */

            case DELETE_BUCKET:
                bucketMgr.deleteBucket(bucketId);
                currState = ExecutionState.SEND_STATUS;
                /*
                ** Fall through
                 */

            case SEND_STATUS:
                currState = ExecutionState.CALLBACK_OPS;
                SendBucketDeleteResponse sendResponse = setupResponseSend();
                sendResponse.event();
                break;

            case CALLBACK_OPS:
                completeCallback.event();
                currState = ExecutionState.EMPTY_STATE;
                break;

            case EMPTY_STATE:
                break;
        }
    }

    /*
     ** This complete() is called when the ReadObjectChunks operation has read all the chunks in and transferred the
     **   data to the client.
     */
    public void complete() {
        LOG.info("SetupBucketDelete[" + requestContext.getRequestId() + "] complete()");

        /*
         ** The following operations are only setup if there was an error reading from the database
         */
        Operation writeToClient = deleteBucketInfoOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        if (writeToClient != null) {
            writeToClient.complete();
        }

        Operation sendResponse = deleteBucketInfoOps.remove(OperationTypeEnum.SEND_BUCKET_DELETE_RESPONSE);
        if (sendResponse != null) {
            sendResponse.complete();
        }

        /*
         ** Call the complete() method for any operations that this one created.
         */
        Collection<Operation> createdOperations = deleteBucketInfoOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        deleteBucketInfoOps.clear();
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
        //LOG.info("SetupBucketDelete[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupBucketDelete[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupBucketDelete[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupBucketDelete[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupBucketDelete[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
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
    private SendBucketDeleteResponse setupResponseSend() {

        /*
         */
        SendBucketDeleteResponse sendResponse = new SendBucketDeleteResponse(requestContext, memoryManager);
        deleteBucketInfoOps.put(sendResponse.getOperationType(), sendResponse);
        BufferManagerPointer clientWritePtr = sendResponse.initialize();

        IoInterface clientConnection = requestContext.getClientConnection();
        WriteToClient writeToClient = new WriteToClient(requestContext, clientConnection, this, clientWritePtr, null);
        deleteBucketInfoOps.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        return sendResponse;
    }


}
