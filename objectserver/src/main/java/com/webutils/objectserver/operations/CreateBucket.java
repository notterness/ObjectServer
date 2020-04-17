package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.PostContentData;
import com.webutils.webserver.mysql.BucketTableMgr;
import com.webutils.webserver.mysql.NamespaceTableMgr;
import com.webutils.webserver.mysql.TenancyTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateBucket implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(CreateBucket.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.CREATE_BUCKET;

    private final RequestContext requestContext;

    private final PostContentData postContentData;

    private final Operation completeCallback;

    /*
    ** Used to insure that the database operations to create the bucket do not get run multiple times.
     */
    private boolean bucketCreated;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public CreateBucket(final RequestContext requestContext, final PostContentData postContentData, final Operation completeCb) {
        this.requestContext = requestContext;
        this.postContentData = postContentData;
        this.completeCallback = completeCb;

        bucketCreated = false;

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
        if (!bucketCreated) {
            WebServerFlavor flavor = requestContext.getWebServerFlavor();

            TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);
            String tenancyUID = tenancyMgr.getTenancyUID("testCustomer", "Tenancy-12345-abcde");

            NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
            String namespaceUID = namespaceMgr.getNamespaceUID("Namespace-xyz-987", tenancyUID);

            BucketTableMgr bucketMgr = new BucketTableMgr(flavor, requestContext.getHttpInfo());
            int status = bucketMgr.createBucketEntry(postContentData, namespaceUID);
            if (status != HttpStatus.OK_200) {
                /*
                **
                 */
                LOG.warn("CreateBucket failed status: " + status);
            }

            completeCallback.complete();

            bucketCreated = true;
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
