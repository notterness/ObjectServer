package com.webutils.objectserver.operations;

import com.webutils.objectserver.common.DeleteChunksObjectServer;
import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.DeleteChunksParams;
import com.webutils.webserver.http.DeleteChunksResponseParser;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ObjectInfo;
import com.webutils.webserver.mysql.ObjectTableMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.SendRequestToService;
import com.webutils.webserver.operations.WriteToClient;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DeleteObjectInfo implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteObjectInfo.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.DELETE_OBJECT;

    private final ObjectServerRequestContext requestContext;

    private final MemoryManager memoryManager;

    private final ObjectInfo objectInfo;

    private final Operation completeCallback;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> deleteObjectInfoOps;

    /*
     ** This is to make the execute() function more manageable
     */
    enum ExecutionState {
        GET_OBJECT_INFO,
        GET_CHUNKS,
        DELETE_CHUNKS,
        DELETE_FROM_DATABASE,
        SEND_STATUS,
        WAIT_FOR_RESPONSE_SEND,
        EMPTY_STATE
    }

    private ExecutionState currState;

    /*
    ** Information used to obtain the information about an object and to delete it
     */
    private final HttpRequestInfo objectPutInfo;
    private final ObjectTableMgr objectMgr;

    /*
     ** The following is used to send the request to delete chunk allocations to the Chunk Mgr Service
     */
    private ServerIdentifier chunkMgrService;
    private SendRequestToService cmdSend;


    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public DeleteObjectInfo(final ObjectServerRequestContext requestContext, final MemoryManager memoryManager,
                            final ObjectInfo objectInfo, final Operation completeCb) {
        this.requestContext = requestContext;
        this.memoryManager = memoryManager;

        this.objectInfo = objectInfo;

        this.completeCallback = completeCb;

        this.deleteObjectInfoOps = new HashMap<>();

        this.currState = ExecutionState.GET_OBJECT_INFO;

        this.objectPutInfo = requestContext.getHttpInfo();

        this.objectMgr = new ObjectTableMgr(requestContext.getWebServerFlavor(), requestContext);
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
            case GET_OBJECT_INFO:
                /*
                ** Need to obtain the following fields from the object before deleting it
                **   opc-request-id
                **   last-modified
                **   version-id
                 */
                if (objectMgr.retrieveObjectInfo(objectPutInfo, objectInfo, requestContext.getTenancyId()) == HttpStatus.OK_200) {
                    currState = ExecutionState.GET_CHUNKS;
                    /*
                     ** Fall through
                     */
                } else {
                    currState = ExecutionState.SEND_STATUS;
                    event();
                    break;
                }

            case DELETE_CHUNKS:
                LOG.info("DeleteObjectInfo DELETE_CHUNKS");

                /*
                 **
                 */
                ServerIdentifierTableMgr serverTableMgr = requestContext.getServerTableMgr();
                if (serverTableMgr != null) {
                    List<ServerIdentifier> servers = new LinkedList<>();
                    boolean found = serverTableMgr.getServer("chunk-mgr-service", servers);
                    if (found) {
                        chunkMgrService = servers.get(0);

                        HttpResponseInfo httpInfo = new HttpResponseInfo(requestContext.getRequestId());

                        chunkMgrService.setHttpInfo(httpInfo);
                    } else {
                        HttpRequestInfo objectCreateInfo = requestContext.getHttpInfo();

                        /*
                         ** Nothing more can be done, might as well return an error
                         */
                        LOG.warn("Unable to obtain address for chunkMgrService");
                        String failureMessage = "{\r\n  \"code\":" + HttpStatus.INTERNAL_SERVER_ERROR_500 +
                                "\r\n  \"message\": \"Unable to obtain address for Chunk Manager Service\"" +
                                "\r\n}";
                        objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, failureMessage);
                        currState = ExecutionState.SEND_STATUS;
                        event();
                        break;
                    }
                } else {
                    HttpRequestInfo objectCreateInfo = requestContext.getHttpInfo();

                    LOG.warn("Unable to obtain serverTableMgr");
                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.INTERNAL_SERVER_ERROR_500 +
                            "\r\n  \"message\": \"Unable to obtain Service Manager Instance\"" +
                            "\r\n}";
                    objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, failureMessage);

                    currState = ExecutionState.SEND_STATUS;
                    event();
                    break;
                }

                DeleteChunksParams params = new DeleteChunksObjectServer(objectInfo.getChunkList());
                params.setOpcClientRequestId(requestContext.getHttpInfo().getOpcClientRequestId());

                DeleteChunksResponseParser parser = new DeleteChunksResponseParser();
                cmdSend = new SendRequestToService(requestContext, memoryManager, chunkMgrService, params, parser, this);
                cmdSend.initialize();

                /*
                 ** Start the process of sending the HTTP Request and the request object to the ChunkMgr service
                 */
                currState = ExecutionState.DELETE_FROM_DATABASE;
                cmdSend.event();
                break;

            case DELETE_FROM_DATABASE:
                int responseStatus = chunkMgrService.getResponseStatus();

                /*
                 ** Clean up from the request to delete the chunks
                 */
                cmdSend.complete();

                if (responseStatus == HttpStatus.OK_200) {
                    /*
                     ** Delete the object information. The critical pieces to access the object information are:
                     **    Tenancy -
                     **    Namespace - The "holder" of all of the buckets for a particular user in a region
                     **    Bucket Name - A place to organize objects
                     **    Object Name - The actual object that is wanted.
                     **    Version Id - An object can have multiple versions
                     */
                    objectMgr.deleteObjectInfo(objectPutInfo, requestContext.getTenancyId());
                } else {
                    HttpRequestInfo objectCreateInfo = requestContext.getHttpInfo();

                    LOG.warn("Request to delete chunks from Chunk Manager Service failed - " + responseStatus);

                    String failureMessage = "{\r\n  \"code\":" + responseStatus +
                            "\r\n  \"message\": \"Request to delete chunks from Chunk Manager Service failed\"" +
                            "\r\n}";
                    objectCreateInfo.setParseFailureCode(responseStatus, failureMessage);
                }
                currState = ExecutionState.SEND_STATUS;
                /*
                ** Fall through
                 */

            case SEND_STATUS:
                currState = ExecutionState.WAIT_FOR_RESPONSE_SEND;
                SendObjectDeleteResponse sendResponse = setupResponseSend();
                sendResponse.event();
                break;

            case WAIT_FOR_RESPONSE_SEND:
                currState = ExecutionState.EMPTY_STATE;
                completeCallback.event();
                break;

            case EMPTY_STATE:
                break;
        }
    }

    public void complete() {

        LOG.info("DeleteObjectInfo[" + requestContext.getRequestId() + "] complete");

        /*
         ** The following operations are only setup if there was an error reading from the database
         */
        Operation writeToClient = deleteObjectInfoOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        if (writeToClient != null) {
            writeToClient.complete();
        }

        Operation sendResponse = deleteObjectInfoOps.remove(OperationTypeEnum.SEND_OBJECT_GET_RESPONSE);
        if (sendResponse != null) {
            sendResponse.complete();
        }

        /*
         ** Call the complete() method for any operations that this one created.
         */
        Collection<Operation> createdOperations = deleteObjectInfoOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        deleteObjectInfoOps.clear();
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
        //LOG.info("DeleteObjectInfo[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("DeleteObjectInfo[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("DeleteObjectInfo[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("DeleteObjectInfo[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("DeleteObjectInfo[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
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
    private SendObjectDeleteResponse setupResponseSend() {

        /*
         ** In the good path, after the HTTP Response is sent back to the client, then start reading in the chunks
         **   that make up the requested object.
         */
        SendObjectDeleteResponse sendResponse = new SendObjectDeleteResponse(requestContext, memoryManager, objectInfo);
        deleteObjectInfoOps.put(sendResponse.getOperationType(), sendResponse);
        BufferManagerPointer clientWritePtr = sendResponse.initialize();

        IoInterface clientConnection = requestContext.getClientConnection();
        WriteToClient writeToClient = new WriteToClient(requestContext, clientConnection, this, clientWritePtr, null);
        deleteObjectInfoOps.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        return sendResponse;
    }


}
