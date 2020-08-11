package com.webutils.accountmgr.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.*;
import com.webutils.webserver.mysql.TenancyTableMgr;
import com.webutils.webserver.mysql.UserTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetAccessToken implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(GetAccessToken.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.GET_ACCESS_TOKEN;

    private final RequestContext requestContext;

    private final GetAccessTokenContent getAccessTokenContent;

    private final Operation completeCallback;

    /*
     ** Used to insure that the database operations to obtain the Access Token do not get run multiple times.
     */
    private boolean tokenObtained;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public GetAccessToken(final RequestContext requestContext, final GetAccessTokenContent getAccessTokenContent,
                      final Operation completeCb) {
        this.requestContext = requestContext;
        this.getAccessTokenContent = getAccessTokenContent;
        this.completeCallback = completeCb;

        tokenObtained = false;

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
        if (!tokenObtained) {
            TenancyTableMgr tenancyMgr = new TenancyTableMgr(requestContext.getWebServerFlavor());
            UserTableMgr userMgr = new UserTableMgr(requestContext.getWebServerFlavor());

            String tenancyName = requestContext.getHttpInfo().getTenancyFromUri();
            String customerName = getAccessTokenContent.getCustomer();
            String userName = getAccessTokenContent.getUser();
            String password = getAccessTokenContent.getPassword();

            int validationStatus = userMgr.validateFields(requestContext.getHttpInfo(), tenancyName, customerName,
                    userName, password);
            if (validationStatus == HttpStatus.OK_200) {
                /*
                 ** Need to obtain the passphrase for this Tenancy to be able to check if the user exists already
                 */
                String passphrase = tenancyMgr.getTenancyPassphrase(customerName, tenancyName);
                int tenancyId = tenancyMgr.getTenancyId(customerName, tenancyName);
                String tenancyUID = tenancyMgr.getTenancyUID(tenancyId);
                if ((passphrase == null) || (tenancyId == -1) || (tenancyUID == null)) {
                    String failureMessage = "{\r\n  \"code\": " + HttpStatus.NOT_FOUND_404 +
                            "\r\n  \"message\": \"GetAccessToken failed: " + tenancyName + "\"" +
                            "\r\n}";
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.NOT_FOUND_404, failureMessage);

                    LOG.warn("GetAccessToken failed - tenancyId, tenancyUID or passphrase not found: " + tenancyName +
                            " " + tenancyId + " " + passphrase + " " + tenancyUID);
                } else {
                    String accessToken = userMgr.getAccessToken(userName, password, passphrase, tenancyId);
                    if (accessToken != null) {
                        requestContext.getHttpInfo().setResponseHeaders(buildSuccessHeader(userMgr, userName, passphrase,
                                tenancyId, accessToken));
                    } else {
                        String failureMessage = "{\r\n  \"code\":" + HttpStatus.FOUND_302 +
                                "\r\n  \"message\": \"AccessToken not present: " + userName + "\"" +
                                "\r\n}";
                        requestContext.getHttpInfo().setParseFailureCode(HttpStatus.FOUND_302, failureMessage);

                        LOG.warn("GetAccessToken failed: " + userName);
                    }
                }
            }
            completeCallback.complete();

            tokenObtained = true;
        }
    }

    public void complete() {
        LOG.info("CreateUser complete");
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
        //LOG.info("CreateUser[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("CreateUser[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("CreateUser[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("CreateUser[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("CreateUser[" + requestContext.getRequestId() +
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
     ** This builds the OK_200 response headers for the POST CreateUser command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   ETag - This is the generated objectUID that is unique to this User
     **   accessToken - The access token that is being returned.
     */
    private String buildSuccessHeader(final UserTableMgr userMgr, final String userName, final String passphrase,
                                      final int tenancyId, final String accessToken) {
        String successHeader;

        String userUID = userMgr.getUserUID(userName, passphrase, tenancyId);
        if (userUID != null) {
            HttpRequestInfo requestInfo = requestContext.getHttpInfo();
            String opcClientRequestId = requestInfo.getOpcClientRequestId();
            int opcRequestId = requestInfo.getRequestId();

            if (opcClientRequestId != null) {
                successHeader = HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId + "\n" +
                        HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\n" +
                        HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + userUID + "\n" +
                        HttpInfo.ACCESS_TOKEN + ": " + accessToken + "\n";
            } else {
                successHeader = HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\n" +
                        HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + userUID + "\n" +
                        HttpInfo.ACCESS_TOKEN + ": " + accessToken + "\n";
            }

            //LOG.info(successHeader);
        } else {
            successHeader = null;
        }

        return successHeader;
    }

}
