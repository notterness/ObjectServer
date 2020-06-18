package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.CreateBucketPostContent;
import com.webutils.webserver.mysql.BucketTableMgr;
import com.webutils.webserver.mysql.NamespaceTableMgr;
import com.webutils.webserver.mysql.UserTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import io.fabric8.openshift.api.model.User;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateBucket implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(CreateBucket.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.CREATE_BUCKET;

    private final RequestContext requestContext;

    private final CreateBucketPostContent createBucketPostContent;

    private final Operation completeCallback;

    /*
    ** Used to insure that the database operations to create the bucket do not get run multiple times.
     */
    private enum ExecutionState {
        GET_CREATED_BY,
        GET_NAMESPACE,
        CREATE_BUCKET,
        CALLBACK_OPS,
        EMPTY_STATE
    }

    private ExecutionState currState;

    /*
    ** Temporary variable
     */
    private String namespaceUID;
    private String createdBy;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public CreateBucket(final RequestContext requestContext, final CreateBucketPostContent createBucketPostContent,
                        final Operation completeCb) {
        this.requestContext = requestContext;
        this.createBucketPostContent = createBucketPostContent;
        this.completeCallback = completeCb;

        this.currState = ExecutionState.GET_CREATED_BY;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

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
     */
    public void execute() {
        WebServerFlavor flavor = requestContext.getWebServerFlavor();

        switch (currState) {
            case GET_CREATED_BY:
                UserTableMgr userMgr = new UserTableMgr(flavor);

                createdBy = userMgr.getCreatedByFromAccessToken(requestContext.getHttpInfo().getAccessToken());
                if (createdBy == null) {
                    LOG.warn("CreateBucket failed - unable to obtain createdBy");
                    String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.BAD_REQUEST_400 + "\"" +
                            "\r\n  \"message\": \"Unknown user from accessToken\"" +
                            "\r\n}";
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);

                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                    break;
                }
                currState = ExecutionState.GET_NAMESPACE;
                /*
                ** Fall through
                 */

            case GET_NAMESPACE:
                NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
                String namespace = createBucketPostContent.getNamespace();

                namespaceUID = namespaceMgr.getNamespaceUID(namespace, requestContext.getTenancyId());
                if (namespaceUID == null) {
                    LOG.warn("CreateBucket failed - unknown namespace: " + namespace);
                    String failureMessage = "{\r\n  \"code\": \"" + HttpStatus.BAD_REQUEST_400 + "\"" +
                            "\r\n  \"message\": \"Unknown namespace - " + namespace + "\"" +
                            "\r\n}";
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);

                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                    break;
                }

                currState = ExecutionState.CREATE_BUCKET;
                /*
                ** Fall through
                 */

            case CREATE_BUCKET:
                BucketTableMgr bucketMgr = new BucketTableMgr(flavor, requestContext.getRequestId(), requestContext.getHttpInfo());
                int status = bucketMgr.createBucketEntry(createBucketPostContent, namespaceUID, createdBy);
                if (status != HttpStatus.OK_200) {
                    /*
                     ** The failure message to be returned is populated in the createBucketEntry() method as it is
                     **   dependent upon the reason for the failure and not a single generic failure message.
                     */
                    LOG.warn("CreateBucket failed status: " + status);
                }

                currState = ExecutionState.CALLBACK_OPS;
                /*
                ** Fall through
                 */

            case CALLBACK_OPS:
                completeCallback.complete();
                currState = ExecutionState.EMPTY_STATE;
                break;

            case EMPTY_STATE:
                break;
        }
    }

    public void complete() {
        LOG.info("CreateBucket complete");
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
        //LOG.info("CreateBucket[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("CreateBucket[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("CreateBucket[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("CreateBucket[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("CreateBucket[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
    }

}
