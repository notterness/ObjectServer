package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.mysql.ObjectInfo;
import com.webutils.webserver.mysql.ObjectTableMgr;
import com.webutils.webserver.mysql.TenancyTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
** This retrieves the information about an Object from the ObjectStorageDb
 */
public class RetrieveObjectInfo implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(RetrieveObjectInfo.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.RETRIEVE_OBJECT_INFO;

    private final RequestContext requestContext;

    private final ObjectInfo objectInfo;

    private final Operation completeCallback;
    private final Operation errorCallback;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public RetrieveObjectInfo(final RequestContext requestContext, final ObjectInfo objectInfo,
                              final Operation completeCb, final Operation errorCb) {
        this.requestContext = requestContext;

        this.objectInfo = objectInfo;

        this.completeCallback = completeCb;
        this.errorCallback = errorCb;
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
        /*
         ** Obtain the object information. The critical pieces to access the object information are:
         **    Tenancy -
         **    Namespace - The "holder" of all of the buckets for a particular user in a region
         **    Bucket Name - A place to organize objects
         **    Object Name - The actual object that is wanted.
         **    Version Id - An object can have multiple versions (TODO: Need to support this)
         */
        HttpRequestInfo objectPutInfo = requestContext.getHttpInfo();

        WebServerFlavor flavor = requestContext.getWebServerFlavor();

        TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);
        String tenancyUID = tenancyMgr.getTenancyUID("testCustomer", "Tenancy-12345-abcde");

        ObjectTableMgr objectMgr = new ObjectTableMgr(flavor, requestContext);
        if (objectMgr.retrieveObjectInfo(objectPutInfo, objectInfo, tenancyUID) == HttpStatus.OK_200) {
            int objectId = objectInfo.getObjectId();
            objectInfo.setEtag(objectMgr.getObjectUID(objectId));

            completeCallback.event();
         } else {
            errorCallback.event();
        }
    }

    public void complete() {
        LOG.info("RetrieveObjectInfo complete");
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

}
