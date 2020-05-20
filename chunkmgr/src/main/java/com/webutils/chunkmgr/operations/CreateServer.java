package com.webutils.chunkmgr.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.CreateServerPostContent;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateServer implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(CreateServer.class);

    /*
    ** THe following are used to build the response header
     */
    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "ETag: ";

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.CREATE_BUCKET;

    private final RequestContext requestContext;

    private final CreateServerPostContent createServerContent;

    private final Operation completeCallback;

    /*
     ** Used to insure that the database operations to create the bucket do not get run multiple times.
     */
    private boolean serverCreated;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public CreateServer(final RequestContext requestContext, final CreateServerPostContent createServerContent,
                        final Operation completeCb) {
        this.requestContext = requestContext;
        this.createServerContent = createServerContent;
        this.completeCallback = completeCb;

        serverCreated = false;

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
        if (!serverCreated) {
            ServerIdentifierTableMgr serverTableMgr = requestContext.getServerTableMgr();

            int status = serverTableMgr.createServer(createServerContent.getServerName(), createServerContent.getServerIP(),
                    createServerContent.getServerPort(), createServerContent.getServerNumChunks(), createServerContent.getStorageTier());
            if (status == HttpStatus.OK_200) {
                int serverId = serverTableMgr.getLastInsertId();
                requestContext.getHttpInfo().setResponseHeaders(buildSuccessHeader(serverTableMgr, serverId));
            } else {
                LOG.warn("CreateServer failed status: " + status);
            }

            completeCallback.complete();

            serverCreated = true;
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
        //LOG.info("CreateServer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("CreateServer[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("CreateServer[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("CreateServer[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("CreateServer[" + requestContext.getRequestId() +
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
     ** This builds the OK_200 response headers for the POST CreateServer command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   ETag - This is the generated objectUID that is unique to this object
     */
    private String buildSuccessHeader(final ServerIdentifierTableMgr serverTableMgr, final int serverId) {
        String successHeader;

        String serverUID = serverTableMgr.getServerUID(serverId);
        if (serverUID != null) {
            HttpRequestInfo requestInfo = requestContext.getHttpInfo();
            String opcClientRequestId = requestInfo.getOpcClientRequestId();
            int opcRequestId = requestInfo.getRequestId();

            /*
             ** FIXME: Need to add in the Location for the full path to the bucket
             */
            if (opcClientRequestId != null) {
                successHeader = SUCCESS_HEADER_1 + opcClientRequestId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n" +
                        SUCCESS_HEADER_3 + serverUID + "\n";
            } else {
                successHeader = SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + serverUID + "\n";
            }

            //LOG.info(successHeader);
        } else {
            successHeader = null;
        }

        return successHeader;
    }

}
