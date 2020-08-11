package com.webutils.accountmgr.operations;

import com.webutils.webserver.http.CreateTenancyPostContent;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.mysql.TenancyTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTenancy implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTenancy.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.CREATE_TENANCY;

    private final RequestContext requestContext;

    private final CreateTenancyPostContent createTenancyContent;

    private final Operation completeCallback;

    /*
     ** Used to insure that the database operations to create the Tenancy do not get run multiple times.
     */
    private boolean tenancyCreated;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public CreateTenancy(final RequestContext requestContext, final CreateTenancyPostContent createTenancyContent,
                        final Operation completeCb) {
        this.requestContext = requestContext;
        this.createTenancyContent = createTenancyContent;
        this.completeCallback = completeCb;

        tenancyCreated = false;

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
        if (!tenancyCreated) {
            TenancyTableMgr tenancyMgr = new TenancyTableMgr(requestContext.getWebServerFlavor());

            /*
             ** First make sure this Tenancy does not already exist
             */
            String tenancyName = createTenancyContent.getTenancyName();
            String customerName = createTenancyContent.getCustomer();
            String passphrase = createTenancyContent.getPassphrase();

            if (tenancyMgr.getTenancyId(customerName, tenancyName) == -1) {
                int status = tenancyMgr.createTenancyEntry(customerName, tenancyName, passphrase);
                if (status == HttpStatus.OK_200) {
                    int tenancyId = tenancyMgr.getTenancyId(customerName, tenancyName);
                    requestContext.getHttpInfo().setResponseHeaders(buildSuccessHeader(tenancyMgr, tenancyId));
                } else {
                    String failureMessage = "{\r\n  \"code\":" + status +
                            "\r\n  \"message\": \"Database error creating tenancy: " + tenancyName + "\"" +
                            "\r\n}";
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, failureMessage);

                    LOG.warn("CreateTenancy failed status: " + status);
                }
            } else {
                String failureMessage = "{\r\n  \"code\":" + HttpStatus.FOUND_302 +
                        "\r\n  \"message\": \"Tenancy already present: " + tenancyName + "\"" +
                        "\r\n}";
                requestContext.getHttpInfo().setParseFailureCode(HttpStatus.FOUND_302, failureMessage);

                LOG.warn("CreateTenancy failed already exists: " + tenancyName);
            }

            completeCallback.complete();

            tenancyCreated = true;
        }
    }

    public void complete() {
        LOG.info("CreateTenancy complete");
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
        //LOG.info("CreateTenancy[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("CreateTenancy[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("CreateTenancy[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("CreateTenancy[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("CreateTenancy[" + requestContext.getRequestId() +
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
     ** This builds the OK_200 response headers for the POST CreateTenancy command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   ETag - This is the generated objectUID that is unique to this Tenancy
     */
    private String buildSuccessHeader(final TenancyTableMgr tenancyMgr, final int tenancyId) {
        String successHeader;

        String tenancyUID = tenancyMgr.getTenancyUID(tenancyId);
        if (tenancyUID != null) {
            HttpRequestInfo requestInfo = requestContext.getHttpInfo();
            String opcClientRequestId = requestInfo.getOpcClientRequestId();
            int opcRequestId = requestInfo.getRequestId();

            if (opcClientRequestId != null) {
                successHeader = HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId + "\n" +
                        HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\n" +
                        HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + tenancyUID + "\n";
            } else {
                successHeader = HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\n" +
                        HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + tenancyUID + "\n";
            }

            //LOG.info(successHeader);
        } else {
            successHeader = null;
        }

        return successHeader;
    }

}
