package com.webutils.storageserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpMethodEnum;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.CloseOutRequest;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.WriteToClient;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class StorageServerDetermineRequest implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(StorageServerDetermineRequest.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.STORAGE_SERVER_DETERMINE_REQUEST;

    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    private final IoInterface clientConnection;

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

    private boolean methodDeterminationDone;

    /*
     ** The following is a map of all of the created Operations to handle sending the Final Status in the following
     **   cases:
     **     - The Storage Server PUT handler
     **     - Errors due to an incorrect HTTP method being sent
     */
    private final Map<OperationTypeEnum, Operation> finalStatusOps;


    public StorageServerDetermineRequest(final RequestContext requestContext, final Map<HttpMethodEnum, Operation> supportedHttpRequests,
                                         final MemoryManager memoryManager, final IoInterface clientConnection) {

        this.requestContext = requestContext;
        this.supportedHttpRequests = supportedHttpRequests;

        this.memoryManager = memoryManager;
        this.clientConnection = clientConnection;

        this.httpRequestInfo = this.requestContext.getHttpInfo();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        methodDeterminationDone = false;

        /*
         ** Map of the operations required to send out the final status
         */
        this.finalStatusOps = new HashMap<>();


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
        if (!methodDeterminationDone) {
            if (requestContext.getHttpParseError()) {
                /*
                 ** Event the send client response operation here so that the final status is sent
                 */
                LOG.warn("StorageServerDetermineRequest[" + requestContext.getRequestId() + "] sending final status HTTP Parse error");

                sendFinalStatus();
            } else {

                /*
                 ** Now, based on the HTTP method, figure out the Operation to event that will setup the sequences for the
                 **   handling of the request.
                 */
                HttpMethodEnum method = httpRequestInfo.getMethod();
                Operation httpRequestSetup = supportedHttpRequests.get(method);
                if (httpRequestSetup != null) {
                    /*
                     ** The operation being run is added to the list for the RequestContext so that it can be cleaned up
                     **   if needed (meaning calling the complete() method).
                     */
                    requestContext.addOperation(httpRequestSetup);

                    LOG.info("StorageServerDetermineRequest[" + requestContext.getRequestId() + "] execute() " + method.toString());
                    httpRequestSetup.initialize();
                    httpRequestSetup.event();
                } else {
                    LOG.info("StorageServerDetermineRequest[" + requestContext.getRequestId() + "] execute() unsupported request " + method.toString());
                    httpRequestInfo.setParseFailureCode(HttpStatus.METHOD_NOT_ALLOWED_405);

                    sendFinalStatus();
                }
            }

            methodDeterminationDone = true;
        } else {
            /*
            ** Do not send the final status for GET operations. The GET operation sends a header and then the chunk
            **   data, so it behaves differently than commands that expect just a response header.
             */
            if (httpRequestInfo.getMethod() != HttpMethodEnum.GET_METHOD) {
                sendFinalStatus();
            }
        }
    }

    public void complete() {
        /*
         */
        if (!finalStatusOps.isEmpty()) {
            LOG.info("StorageServerDetermineRequest cleanup final status ops");
            Operation operation;

            operation = finalStatusOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
            operation.complete();

            operation = finalStatusOps.remove(OperationTypeEnum.SEND_FINAL_STATUS);
            operation.complete();

            operation = finalStatusOps.remove(OperationTypeEnum.CLOSE_OUT_REQUEST);
            operation.complete();
        } else {
            LOG.info("StorageServerDetermineRequest complete()");
        }
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
        //LOG.info("StorageServerDetermineRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("StorageServerDetermineRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("StorageServerDetermineRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("StorageServerDetermineRequest[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("StorageServerDetermineRequest[" + requestContext.getRequestId() +
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

    private void sendFinalStatus() {
        /*
         ** The StorageServerSendFinalStatus, WriteToClient and CloseOutRequest are tied together. The
         **   StorageServerSendFinalStatus is responsible for building the final HTTP status response to the client in
         **   the case of errors and for the PUT operation.
         **   Once the cleanup is performed, then the RequestContext is added back to the free list so
         **   it can be used to handle a new request.
         */
        StorageServerSendFinalStatus sendFinalStatus = new StorageServerSendFinalStatus(requestContext, memoryManager);
        finalStatusOps.put(sendFinalStatus.getOperationType(), sendFinalStatus);
        BufferManagerPointer clientWritePtr = sendFinalStatus.initialize();

        CloseOutRequest closeRequest = new CloseOutRequest(requestContext);
        finalStatusOps.put(closeRequest.getOperationType(), closeRequest);
        closeRequest.initialize();

        WriteToClient writeToClient = new WriteToClient(requestContext, clientConnection, closeRequest, clientWritePtr, null);
        finalStatusOps.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        /*
        ** Now kick off the sending of the final status
         */
        sendFinalStatus.event();
    }

}
