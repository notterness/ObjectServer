package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.HttpMethodEnum;
import com.webutils.webserver.mysql.UserTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DetermineRequest implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(DetermineRequest.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.DETERMINE_REQUEST;

    private final RequestContext requestContext;

    /*
     ** This is to make the execute() function more manageable
     */
    private enum ExecutionState {
        VALIDATE,
        OBTAIN_TENANCY,
        EXECUTE_METHOD,
        CALLBACK_OPS,
        EMPTY_STATE
    }

    private ExecutionState currState;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** The following map is passed into the RequestContext and it provides a list of all of the Operations that
     **   provide the initial handler for an HTTP Request type. This is setup at the start of execution and
     **   is only populated with handler operations (i.e. V2 PUT).
     */
    private final Map<HttpMethodEnum, Operation> supportedHttpRequests;

    private final HttpRequestInfo httpRequestInfo;

    private Operation sendFinalStatus;

    public DetermineRequest(final RequestContext requestContext, final Map<HttpMethodEnum, Operation> supportedHttpRequests) {

        this.requestContext = requestContext;
        this.supportedHttpRequests = supportedHttpRequests;

        this.httpRequestInfo = this.requestContext.getHttpInfo();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        this.currState = ExecutionState.VALIDATE;
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

    /*
    ** This execute() method does two distinct things.
    **    1) First it uses the information in the HttpInfo object to determine the HTTP Request to be handled.
    **       There is a setup request operation for each type of HTTP Request and that is then initialized() and
    **       started via the event() method. At that point, the DetermineRequest operation sits idle until the
    **       HTTP Request is completed and the DetermineRequest operation has its event() method called again.
    **    2) When DetermineRequest has its event() method called a second time, it then must send the HTTP
    **       Response (via the SendFinalStatus operation) to the client.
     */
    public void execute() {
        switch (currState) {
            case VALIDATE:
                sendFinalStatus = requestContext.getOperation(OperationTypeEnum.SEND_FINAL_STATUS);
                if (sendFinalStatus == null) {
                    LOG.info("DetermineRequest[" + requestContext.getRequestId() + "] sendFinalStatus null");
                    return;
                }
                currState = ExecutionState.OBTAIN_TENANCY;
                /*
                ** Fall through
                 */

            case OBTAIN_TENANCY:
                /*
                 ** Obtain the tenancyId
                 */
                String accessToken = httpRequestInfo.getAccessToken();

                if (accessToken != null) {
                    UserTableMgr userMgr = new UserTableMgr(requestContext.getWebServerFlavor());
                    requestContext.setTenancyId(userMgr.getTenancyFromAccessToken(accessToken));

                    currState = ExecutionState.EXECUTE_METHOD;
                    /*
                     ** Fall through
                     */
                } else {
                    /*
                    ** This is a permissions error
                     */
                    LOG.info("DetermineRequest[" + requestContext.getRequestId() + "] accessToken not provided");
                    httpRequestInfo.setParseFailureCode(HttpStatus.NETWORK_AUTHENTICATION_REQUIRED_511);

                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                    break;
                }

            case EXECUTE_METHOD:
                currState = ExecutionState.CALLBACK_OPS;
                if (requestContext.getHttpParseError()) {
                    /*
                     ** Event the send client response operation here so that the final status is sent
                     */
                    LOG.warn("DetermineRequest[" + requestContext.getRequestId() + "] sending final status HTTP Parse error");
                } else {

                    /*
                     ** Now, based on the HTTP method, figure out the Operation to event that will setup the sequences for the
                     **   handling of the request.
                     */
                    HttpMethodEnum method = httpRequestInfo.getMethod();
                    Operation httpMethod = supportedHttpRequests.get(method);
                    if (httpMethod != null) {
                        /*
                         ** The operation being run is added to the list for the RequestContext so that it can be cleaned up
                         **   if needed (meaning calling the complete() method).
                         */
                        requestContext.addOperation(httpMethod);

                        LOG.info("DetermineRequest[" + requestContext.getRequestId() + "] execute(1) " + method.toString());
                        httpMethod.initialize();
                        httpMethod.event();
                        break;
                    } else {
                        LOG.info("DetermineRequest[" + requestContext.getRequestId() + "] execute(1) unsupported request " + method.toString());
                        httpRequestInfo.setParseFailureCode(HttpStatus.METHOD_NOT_ALLOWED_405);
                    }
                }
                /*
                ** Fall through is all cases except when the httpMethod is called (meaning initialize() and event())
                 */

            case CALLBACK_OPS:
                HttpMethodEnum method = httpRequestInfo.getMethod();
                LOG.info("DetermineRequest[" + requestContext.getRequestId() + "] execute(2) " + method.toString());

                /*
                ** Call sendFinalStatus() for all error cases.
                ** The ObjectGet method handles sending out the good status prior to it sending the object data, so if
                **   there is not an error, sendFinalStatus() must not be called here.
                ** The DeleteObject and DeleteBucket methods return NO_CONTENT_204 if they are successful and they both
                **   handle returning the good status.
                 */
                Operation closeOutRequest = requestContext.getOperation(OperationTypeEnum.CLOSE_OUT_REQUEST);

                switch (method) {
                    case DELETE_METHOD:
                    case DELETE_BUCKET:
                        if (closeOutRequest != null) {
                            /*
                             ** This will finish up this request
                             */
                            closeOutRequest.event();
                        } else {
                            LOG.info("DetermineRequest[" + requestContext.getRequestId() + "] closeOutRequest null");
                        }
                        break;

                    case GET_METHOD:
                        if (httpRequestInfo.getParseFailureCode() != HttpStatus.OK_200) {
                            sendFinalStatus.event();
                        } else if (closeOutRequest != null) {
                            /*
                             ** This will finish up this request
                             */
                            closeOutRequest.event();
                        } else {
                            LOG.info("DetermineRequest[" + requestContext.getRequestId() + "] closeOutRequest null");
                        }
                        break;

                    default:
                        sendFinalStatus.event();
                        break;

                }

                currState = ExecutionState.EMPTY_STATE;
                break;

            case EMPTY_STATE:
                break;
        }
    }

    public void complete() {
        /*
        ** This does not have anything that needs to be released or cleaned up, so this is just an
        **   empty method for now.
         */
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
        //LOG.info("DetermineRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("DetermineRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("DetermineRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("DetermineRequest[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("DetermineRequest[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("      No BufferManagerPointers");
        LOG.info("");
    }

}
