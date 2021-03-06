package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.GetObjectParams;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/*
** This implements the logic to send the GetObject command to the Object Server. It provides the back end for the
**   CLI command.
 */
public class ClientGetObject implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ClientGetObject.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_GET_OBJECT;


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

    private final GetObjectParams requestParams;

    private final MemoryManager memoryManager;

    /*
     ** This is to make the execute() function more manageable
     */
    private enum ExecutionState {
        SETUP_OBJECT_READ_OPS,
        WAITING_FOR_CONN_COMP,
        WAITING_FOR_RESPONSE_HEADER,
        READ_CONTENT_DATA,
        READ_ERROR_CONTENT_DATA,
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
    private final Map<OperationTypeEnum, Operation> requestHandlerOps;

    /*
     ** The following is the connection used to communicate with the Storage Server
     */
    private final IoInterface objectServerConn;

    /*
     ** The updater used to manage the Md5 digest and its result
     */
    private ResponseMd5ResultHandler updater;

    /*
     ** SetupChunkWrite is called at the beginning of each chunk (128MB) block of data. This is what sets
     **   up the calls to obtain the VON information and the meta-data write to the database.
     */
    public ClientGetObject(final ClientInterface clientGetInterface, final ClientRequestContext requestContext,
                           final MemoryManager memoryManager, final ServerIdentifier server,
                           final GetObjectParams getRequestParams) {

        this.clientInterface = clientGetInterface;
        this.requestContext = requestContext;
        this.objectServer = server;
        this.requestParams = getRequestParams;
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
        requestHandlerOps = new HashMap<>();

        currState = ExecutionState.SETUP_OBJECT_READ_OPS;

        LOG.info("ClientGetObject addr: " + objectServer.getServerIpAddress().toString() + " port: " +
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
                    LOG.warn("ClientGetObject failure: " + status + " addr: " + objectServer.getServerIpAddress().toString() +
                            " port: " + objectServer.getServerTcpPort());

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
                        ** Fall through to the READ_CONTENT_DATA state
                         */
                    } else if (status != -1) {
                        /*
                         ** Some sort of an error response, check if there is a response body to read in
                         */
                        currState = ExecutionState.READ_ERROR_CONTENT_DATA;
                        event();
                        break;
                    } else {
                        /*
                         ** Should not be here if the getHeaderComplete() is true as a -1 status indicates that a
                         **   response header has not been received.
                         */
                        LOG.warn("WAITING_FOR_RESPONSE_HEADER should not be here status: " + status);
                        break;
                    }
                } else {
                    /*
                    ** Still waiting for the response header to arrive
                     */
                    break;
                }

            case READ_CONTENT_DATA:
                LOG.info("READ_CONTENT_DATA");
                if (startContentRead()) {
                    currState = ExecutionState.CONTENT_PROCESSED;
                } else {
                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                }
                break;

            case READ_ERROR_CONTENT_DATA:
                LOG.info("READ_ERROR_CONTENT_DATA");
                currState = ExecutionState.DELETE_FILE;
                if (!startErrorContentRead()) {
                    event();
                }
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
                 ** Display the results
                 */
                requestParams.outputResults(httpInfo);

                /*
                 ** Now call back the client interface to complete the CLI operation
                 */
                currState = ExecutionState.EMPTY_STATE;
                clientInterface.clientRequestCompleted(httpInfo.getResponseStatus());
                break;

            case DELETE_FILE:
                {
                    String fileName = requestParams.getFilePathName();

                    /*
                    ** Some sort of error, so need to delete the file
                     */
                    LOG.warn("Deleting file: " + fileName + " due to error: " + requestContext.getHttpParseStatus());

                    Path filePath = Paths.get(fileName);
                    try {
                        Files.deleteIfExists(filePath);
                    } catch (IOException ex) {
                        LOG.warn("Failure deleting file: " + fileName + " exception: " + ex.getMessage());
                    }

                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                }
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

        LOG.info("[ClientGetObject complete() addr: " + objectServer.getServerIpAddress().toString() + " port: " +
                objectServer.getServerTcpPort());

        /*
         ** The following must be called in order to make sure that the BufferManagerPointer
         **   dependencies are torn down in the correct order. The pointers in
         **   headerWriter are dependent upon the pointers in headerBuilder
         */
        Operation headerWriter = requestHandlerOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        headerWriter.complete();

        Operation headerBuilder = requestHandlerOps.remove(OperationTypeEnum.BUILD_OBJECT_GET_HEADER);
        headerBuilder.complete();

        Operation processResponse = requestHandlerOps.remove(OperationTypeEnum.RESPONSE_HANDLER);
        if (processResponse != null) {
            processResponse.complete();
        }

        /*
         ** Check if the Md5 and Decrypt operations are present and if so, call complete for them. These may not be
         **   present if the chunk read from the Storage Server failed early.
         **
         ** NOTE: The call to complete() for the Md5Digest is handled when it completes the digest computation.
         */
        requestHandlerOps.remove(OperationTypeEnum.COMPUTE_MD5_DIGEST);

        Operation writeObjectToFile = requestHandlerOps.remove(OperationTypeEnum.WRITE_OBJECT_TO_FILE);
        if (writeObjectToFile != null) {
            writeObjectToFile.complete();
        }

        /*
         ** Check if the ConvertRespBodyToString operation is present. It may not be in certain error conditions.
         */
        Operation convertBodyToStr = requestHandlerOps.remove(OperationTypeEnum.CONVERT_RESP_BODY_TO_STR);
        if (convertBodyToStr != null) {
            convertBodyToStr.complete();
        }

        Operation readBuffer = requestHandlerOps.remove(OperationTypeEnum.READ_BUFFER);
        readBuffer.complete();

        /*
         ** Remove the HandleChunkReadConnError operation from the createdOperations list. This never has its
         **   complete() method called, so it is best just to remove it.
         */
        requestHandlerOps.remove(OperationTypeEnum.HANDLE_CLIENT_ERROR);

        /*
         ** Call the complete() methods for all of the Operations created to handle the chunk write that did not have
         **   ordering dependencies due to the registrations with the BufferManager(s).
         */
        Collection<Operation> createdOperations = requestHandlerOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        requestHandlerOps.clear();

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
        //LOG.info("ClientGetObject markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ClientGetObject markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ClientGetObject markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ClientGetObject markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ClientGetObject hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("  -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = requestHandlerOps.values();
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
        requestHandlerOps.put(errorHandler.getOperationType(), errorHandler);

        /*
         ** The GET Header must be written to the Object Server so that the data can be read in
         */
        BuildObjectGetHeader headerBuilder = new BuildObjectGetHeader(requestContext, requestBufferManager,
                addBufferPointer, requestParams);
        requestHandlerOps.put(headerBuilder.getOperationType(), headerBuilder);
        BufferManagerPointer writePointer = headerBuilder.initialize();

        WriteToClient headerWriter = new WriteToClient(requestContext, objectServerConn, this,
                writePointer, objectServer);
        requestHandlerOps.put(headerWriter.getOperationType(), headerWriter);
        headerWriter.initialize();

        /*
         ** For each Storage Server, setup a ConnectComplete operation that is used when the NIO
         **   connection is made with the StorageServer.
         */
        List<Operation> operationList = new LinkedList<>();
        operationList.add(headerBuilder);
        operationList.add(responseBufferMetering);
        ConnectComplete connectComplete = new ConnectComplete(requestContext, operationList, objectServer.getServerTcpPort());
        requestHandlerOps.put(connectComplete.getOperationType(), connectComplete);

        /*
         ** Setup the operations to read in the HTTP Response header and process it
         */
        ReadBuffer readBuffer = new ReadBuffer(requestContext, responseBufferManager, respBufferPointer, objectServerConn);
        requestHandlerOps.put(readBuffer.getOperationType(), readBuffer);
        httpBufferPointer = readBuffer.initialize();


        ResponseHandler httpRespHandler = new ResponseHandler(requestContext, responseBufferManager, httpBufferPointer,
                responseBufferMetering, this, objectServer);
        requestHandlerOps.put(httpRespHandler.getOperationType(), httpRespHandler);
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

    /*
     ** This is used to read in the content returned with the response headers. If the Content-Length is 0, it will not
     **   setup anything and will simply return false.
     */
    private boolean startContentRead() {
        /*
         ** Tear down the response header reader and handler as they are no longer needed.
         **
         ** The following must be called in order to make sure that the BufferManagerPointer dependencies are torn down
         **   in the correct order. The pointers in httpRespHandler are dependent upon the pointers in readRespBuffer.
         */
        Operation httpRespHandler = requestHandlerOps.remove(OperationTypeEnum.RESPONSE_HANDLER);
        httpRespHandler.complete();

        int objectSize = httpInfo.getContentLength();
        if (objectSize != 0) {
            /*
             ** The next Operations are run once the response header has been received. The routines are to
             **   compute the Md5 digest and decrypt all the data into the chunk buffer.
             */
            List<Operation> opsToRun = new LinkedList<>();
            opsToRun.add(this);

            updater = new ResponseMd5ResultHandler(requestContext);
            ComputeMd5Digest md5Digest = new ComputeMd5Digest(requestContext, opsToRun, httpBufferPointer,
                    responseBufferManager, updater, httpInfo.getContentLength());
            requestHandlerOps.put(md5Digest.getOperationType(), md5Digest);
            md5Digest.initialize();

            WriteObjectToFile writeObjectToFile = new WriteObjectToFile(requestContext, httpBufferPointer,
                    responseBufferMetering, requestParams.getFilePathName(), objectSize,this);
            requestHandlerOps.put(writeObjectToFile.getOperationType(), writeObjectToFile);
            writeObjectToFile.initialize();

            return true;
        }

        LOG.warn("startContentRead() Content-Length is 0");
        return false;
    }

    /*
     ** This is used to read in the content returned with the response headers. If the Content-Length is 0, it will not
     **   setup anything and will simply return false.
     */
    private boolean startErrorContentRead() {
        /*
         ** Tear down the response header reader and handler as they are no longer needed.
         **
         ** The following must be called in order to make sure that the BufferManagerPointer dependencies are torn down
         **   in the correct order. The pointers in httpRespHandler are dependent upon the pointers in readRespBuffer.
         */
        Operation httpRespHandler = requestHandlerOps.remove(OperationTypeEnum.RESPONSE_HANDLER);
        httpRespHandler.complete();

        if (httpInfo.getContentLength() != 0) {
            /*
             ** The next Operations are run once the response header has been received. This is to convert the bytes
             **   read in into a String.
             */
            ConvertRespBodyToString convertToStr = new ConvertRespBodyToString(requestContext, httpBufferPointer,
                    responseBufferMetering, httpInfo, this);
            requestHandlerOps.put(convertToStr.getOperationType(), convertToStr);
            convertToStr.initialize();

            return true;
        }

        LOG.info("startContentRead() Content-Length is 0");
        return false;
    }

}
