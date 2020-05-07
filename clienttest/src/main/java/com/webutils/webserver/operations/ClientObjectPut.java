package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.PutObjectParams;
import com.webutils.webserver.common.ResponseMd5ResultHandler;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.manual.ClientInterface;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class ClientObjectPut implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ClientObjectPut.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_OBJECT_PUT;


    private final int OBJECT_SERVER_HEADER_BUFFER_COUNT = 4;

    /*
     ** The overall controlling object that allocated the reuqest context and threads.
     */
    private final ClientInterface clientInterface;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final ClientRequestContext requestContext;

    /*
     ** The ServerIdentifier is this object reader's unique identifier. It identifies the Object Server through the
     **   IP address and Port number.
     */
    private final ServerIdentifier objectServer;

    private final PutObjectParams requestParams;

    private final MemoryManager memoryManager;

    /*
     ** This is to make the execute() function more manageable
     */
    enum ExecutionState {
        SETUP_OBJECT_READ_OPS,
        WAITING_FOR_CONN_COMP,
        WAITING_FOR_RESPONSE_HEADER,
        READ_CONTENT_DATA,
        CONTENT_PROCESSED,
        DELETE_FILE,
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

    private BufferManager requestBufferManager;
    private BufferManager responseBufferManager;

    private BufferManagerPointer addBufferPointer;

    /*
     ** The response buffer metering is used by the WritObjectToFile operation
     */
    private BufferReadMetering responseBufferMetering;

    private BufferManagerPointer httpBufferPointer;

    /*
     ** The HttpResponseInfo is unique to this read chunk operation as the response coming back is only for one chunk
     **   worth of data.
     */
    private final HttpResponseInfo httpInfo;


    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> requestHandlerOperations;

    /*
     ** The following is the connection used to communicate with the Storage Server
     */
    private final IoInterface objectServerConn;

    private boolean serverConnectionClosedDueToError;

    /*
     ** The updater used to manage the Md5 digest and its result
     */
    private ResponseMd5ResultHandler updater;

    /*
     ** SetupChunkWrite is called at the beginning of each chunk (128MB) block of data. This is what sets
     **   up the calls to obtain the VON information and the meta-data write to the database.
     */
    public ClientObjectPut(final ClientInterface clientInterface, final ClientRequestContext requestContext,
                           final MemoryManager memoryManager, final ServerIdentifier server,
                           final PutObjectParams putParams) {

        this.clientInterface = clientInterface;
        this.requestContext = requestContext;
        this.objectServer = server;
        this.requestParams = putParams;
        this.memoryManager = memoryManager;

        /*
         ** Obtain the HttpResponseInfo for this request.
         */
        this.httpInfo = server.getHttpInfo();

        this.objectServerConn = requestContext.getClientConnection();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        requestHandlerOperations = new HashMap<>();

        currState = ExecutionState.SETUP_OBJECT_READ_OPS;

        serverConnectionClosedDueToError = false;

        LOG.info("ClientObjectGet[" + requestContext.getRequestId() + "] addr: " +
                objectServer.getServerIpAddress().toString() + " port: " +
                objectServer.getServerTcpPort());
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
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
     **
     */
    public void execute() {
        switch (currState) {
            case SETUP_OBJECT_READ_OPS:
                if (setupReadOps()) {
                    currState = ExecutionState.WAITING_FOR_CONN_COMP;
                } else {
                    /*
                     ** This is the case where the connection to the Storage Server could not be established
                     */
                    objectServer.setResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                }
                break;

            case WAITING_FOR_CONN_COMP:
            {
                /*
                 ** First check if all the request has been sent to the ObjectServer (meaning the connection
                 **   was made and the data has been written)
                 */
                int status;
                if (requestContext.hasHttpRequestBeenSent(objectServer)) {

                    requestContext.removeHttpRequestSent(objectServer);
                    currState = ExecutionState.WAITING_FOR_RESPONSE_HEADER;
                    /*
                     ** Fall through to handle the case where the connection was setup and the response header was
                     **   received prior to this being scheduled.
                     */
                } else if ((status = requestContext.getStorageResponseResult(objectServer)) != HttpStatus.OK_200) {
                    LOG.warn("ClientObjectGet[" + requestContext.getRequestId() + "] failure: " + status + " addr: " +
                            objectServer.getServerIpAddress().toString() + " port: " + objectServer.getServerTcpPort());

                    objectServer.setResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                    break;
                } else {
                    LOG.info("WAIT_FOR_CONN_COMP - request not sent yet");
                    break;
                }
            }

            case WAITING_FOR_RESPONSE_HEADER:
                if (httpInfo.getHeaderComplete())
                {
                    int status = httpInfo.getResponseStatus();

                    LOG.info("WAITING_FOR_RESPONSE_HEADER status: " + status);

                    if (status == HttpStatus.OK_200) {
                        currState = ExecutionState.READ_CONTENT_DATA;
                        /*
                         ** This code path could fall through instead of running through the scheduler
                         */
                        event();
                    } else if (status != -1) {
                        /*
                         ** Some sort of an error response
                         */
                        objectServer.setResponseStatus(HttpStatus.OK_200);
                        currState = ExecutionState.CALLBACK_OPS;
                        event();
                    }
                }
                break;

            case READ_CONTENT_DATA:
                LOG.info("READ_CONTENT_DATA");
                currState = ExecutionState.CONTENT_PROCESSED;
                startContentRead();
                break;

            case CONTENT_PROCESSED:
                boolean md5Complete = updater.getMd5DigestComplete();
                boolean parseError = requestContext.getHttpParseError();
                boolean allDataWritten = requestContext.getAllObjectDataWritten();
                LOG.info("CONTENT_PROCESSED md5Complete: " + md5Complete + " allDataWritten: " + allDataWritten +
                        " parseError: " + parseError);

                if (md5Complete && allDataWritten) {
                    if (updater.checkContentMD5(objectServer.getMd5Digest())) {
                        objectServer.setResponseStatus(HttpStatus.OK_200);

                    } else {
                        objectServer.setResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY_422);
                    }
                    LOG.info("chunk read from Storage Server complete");
                    /*
                     ** Fall through
                     */
                    currState = ExecutionState.CALLBACK_OPS;
                } else if (parseError) {
                    /*
                     ** Need to callback the higher level. The file will have already been deleted.
                     */
                } else {
                    /*
                     ** Still waiting for the md5 operations to complete
                     */
                    break;
                }

            case CALLBACK_OPS:
                LOG.info("SetupChunkRead CALLBACK_OPS");

                /*
                 ** Now call back the
                 */
                currState = ExecutionState.EMPTY_STATE;
                clientInterface.clientRequestCompleted(httpInfo.getResponseStatus());
                break;

            case DELETE_FILE:
                break;

            case EMPTY_STATE:
                break;

        }
    }

    /*
     ** This is called to cleanup the SetupChunkWrite and will tear down all the BufferManagerPointers and
     **   release the ByteBuffer(s) associated with the storageServerBufferManager.
     */
    public void complete() {

        LOG.info("ClientObjectGet[" + requestContext.getRequestId() + "] complete() addr: " +
                objectServer.getServerIpAddress().toString() + " port: " +
                objectServer.getServerTcpPort());

        /*
         ** The following must be called in order to make sure that the BufferManagerPointer
         **   dependencies are torn down in the correct order. The pointers in
         **   headerWriter are dependent upon the pointers in headerBuilder
         */
        Operation headerWriter = requestHandlerOperations.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        headerWriter.complete();

        Operation headerBuilder = requestHandlerOperations.remove(OperationTypeEnum.BUILD_OBJECT_GET_HEADER);
        headerBuilder.complete();

        Operation processResponse = requestHandlerOperations.remove(OperationTypeEnum.RESPONSE_HANDLER);
        if (processResponse != null) {
            processResponse.complete();
        }

        /*
         ** Check if the Md5 and Decrypt operations are present and if so, call complete for them. These may not be
         **   present if the chunk read from the Storage Server failed early.
         **
         ** NOTE: The call to complete() for the Md5Digest is handled when it completes the digest computation.
         */
        requestHandlerOperations.remove(OperationTypeEnum.COMPUTE_MD5_DIGEST);

        Operation writeObjectToFile = requestHandlerOperations.remove(OperationTypeEnum.WRITE_OBJECT_TO_FILE);
        if (writeObjectToFile != null) {
            writeObjectToFile.complete();
        }

        Operation readBuffer = requestHandlerOperations.remove(OperationTypeEnum.READ_BUFFER);
        readBuffer.complete();

        /*
         ** Remove the HandleChunkReadConnError operation from the createdOperations list. This never has its
         **   complete() method called, so it is best just to remove it.
         */
        requestHandlerOperations.remove(OperationTypeEnum.HANDLE_CLIENT_ERROR);

        /*
         ** Call the complete() methods for all of the Operations created to handle the chunk write that did not have
         **   ordering dependencies due to the registrations with the BufferManager(s).
         */
        Collection<Operation> createdOperations = requestHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        requestHandlerOperations.clear();

        /*
         ** Close out the connection used to communicate with the Storage Server. Then
         ** clear out the reference to the connection so it may be released back to the pool.
         */
        if (!serverConnectionClosedDueToError) {
            objectServerConn.closeConnection();
        }

        requestContext.releaseConnection(objectServerConn);

        /*
         ** Return the allocated buffers that were used to send the GET Request to the Storage Server
         */
        requestBufferManager.reset(addBufferPointer);
        for (int i = 0; i < OBJECT_SERVER_HEADER_BUFFER_COUNT; i++) {
            ByteBuffer buffer = requestBufferManager.getAndRemove(addBufferPointer);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer, requestBufferManager);
            } else {
                LOG.info("SetupChunkRead[" + requestContext.getRequestId() + "] null buffer addBufferPointer index: " + addBufferPointer.getCurrIndex());
            }
        }

        requestBufferManager.unregister(addBufferPointer);
        addBufferPointer = null;

        requestBufferManager.reset();
        requestBufferManager = null;

        /*
         ** Return the allocated buffers that were used to receive the HTTP Response and the data
         **   from the Storage Server
         */
        responseBufferMetering.complete();
        responseBufferManager = null;

        /*
         ** Clear the HTTP Request sent for this Storage Server
         */
        requestContext.removeHttpRequestSent(objectServer);

        /*
         ** remove the HttpResponseInfo association from the ServerIdentifier
         */
        objectServer.setHttpInfo(null);

    }

    public void connectionCloseDueToError() {
        serverConnectionClosedDueToError = true;
    }

    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when an operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ClientObjectGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ClientObjectGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ClientObjectGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ClientObjectGet[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ClientObjectGet[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("  -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = requestHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

    /*
     ** This sets up the operations and all of their dependencies to read the content data in from the Object Server.
     **
     ** It will return false if the setup of the connection to the Object Server fails
     */
    private boolean setupReadOps() {
        /*
         ** Create a BufferManager with two required entries to send the HTTP GET Request header to the
         **   Object Server.
         */
        requestBufferManager = requestContext.getClientWriteBufferManager();

        /*
         ** Allocate ByteBuffer(s) for the GET request header that will be sent to the Object Server
         */
        addBufferPointer = requestBufferManager.register(this);
        requestBufferManager.bookmark(addBufferPointer);
        for (int i = 0; i < OBJECT_SERVER_HEADER_BUFFER_COUNT; i++) {
            ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, requestBufferManager,
                    operationType);

            requestBufferManager.offer(addBufferPointer, buffer);
        }

        requestBufferManager.reset(addBufferPointer);

        /*
         ** Create a BufferManager to accept the response from the Object Server. This BufferManager is used
         **   to hold the buffers as they are read in from the client. From there, the buffers are run through an
         **   Md5 digest and written out to the file that was passed in by the client.
         **
         */
        responseBufferManager = requestContext.getClientReadBufferManager();

        /*
         ** Allocate ByteBuffer(s) to read in the response from the Storage Server. By using a metering operation, the
         **   setup for the reading of the Storage Server response header can be be deferred until the TCP connection to the
         **   Storage Server is successful.
         */
        responseBufferMetering = new BufferReadMetering(requestContext, memoryManager);
        BufferManagerPointer respBufferPointer = responseBufferMetering.initialize();

        /*
         ** For each Storage Server, setup a HandleChunkWriteConnError operation that is used when there
         **   is an error communicating with the StorageServer.
         */
        HandleClientError errorHandler = new HandleClientError(requestContext, this);
        requestHandlerOperations.put(errorHandler.getOperationType(), errorHandler);

        /*
         ** The PUT Header must be written to the Object Server so that the data can be read in
         */
        BuildObjectGetHeader headerBuilder = new BuildObjectGetHeader(requestContext, requestBufferManager,
                addBufferPointer, requestParams);
        requestHandlerOperations.put(headerBuilder.getOperationType(), headerBuilder);
        BufferManagerPointer writePointer = headerBuilder.initialize();

        WriteToClient headerWriter = new WriteToClient(requestContext, objectServerConn, this,
                writePointer, objectServer);
        requestHandlerOperations.put(headerWriter.getOperationType(), headerWriter);
        headerWriter.initialize();

        /*
         ** For each Storage Server, setup a ConnectComplete operation that is used when the NIO
         **   connection is made with the StorageServer.
         */
        List<Operation> operationList = new LinkedList<>();
        operationList.add(headerBuilder);
        operationList.add(responseBufferMetering);
        ConnectComplete connectComplete = new ConnectComplete(requestContext, operationList, objectServer.getServerTcpPort());
        requestHandlerOperations.put(connectComplete.getOperationType(), connectComplete);

        /*
         ** Setup the operations to read in the HTTP Response header and process it
         */
        ReadBuffer readBuffer = new ReadBuffer(requestContext, responseBufferManager, respBufferPointer, objectServerConn);
        requestHandlerOperations.put(readBuffer.getOperationType(), readBuffer);
        httpBufferPointer = readBuffer.initialize();


        ResponseHandler httpRespHandler = new ResponseHandler(requestContext, responseBufferManager, httpBufferPointer,
                responseBufferMetering, this, objectServer);
        requestHandlerOperations.put(httpRespHandler.getOperationType(), httpRespHandler);
        httpRespHandler.initialize();

        /*
         ** Now open a initiator connection to write encrypted buffers out of.
         */
        if (!objectServerConn.startInitiator(objectServer.getServerIpAddress(),
                objectServer.getServerTcpPort(), connectComplete, errorHandler)) {
            /*
             ** This means the SocketChannel could not be opened. Need to indicate a problem
             **   with the Storage Server and clean up this SetupChunkWrite.
             ** Set the error to indicate that the Storage Server cannot be reached.
             **
             ** TODO: Add a test case for the startInitiator failing to make sure the cleanup
             **   is properly handled.
             */
            complete();
            return false;
        }

        return true;
    }

    private void startContentRead() {
        /*
         ** Tear down the response header reader and handler as they are no longer needed.
         **
         ** The following must be called in order to make sure that the BufferManagerPointer dependencies are torn down
         **   in the correct order. The pointers in httpRespHandler are dependent upon the pointers in readRespBuffer.
         */
        Operation httpRespHandler = requestHandlerOperations.remove(OperationTypeEnum.RESPONSE_HANDLER);
        httpRespHandler.complete();

        /*
         ** The next Operations are run once the response header has been received. The routines are to
         **   compute the Md5 digest and decrypt all the data into the chunk buffer.
         */
        List<Operation> opsToRun = new LinkedList<>();
        opsToRun.add(this);

        updater = new ResponseMd5ResultHandler(requestContext);
        ComputeMd5Digest md5Digest = new ComputeMd5Digest(requestContext, opsToRun, httpBufferPointer,
                responseBufferManager, updater, httpInfo.getContentLength());
        requestHandlerOperations.put(md5Digest.getOperationType(), md5Digest);
        md5Digest.initialize();

        WriteObjectToFile writeObjectToFile = new WriteObjectToFile(requestContext, httpBufferPointer,
                responseBufferMetering, requestParams.getFilePathName(),this);
        requestHandlerOperations.put(writeObjectToFile.getOperationType(), writeObjectToFile);
        writeObjectToFile.initialize();
    }

}
