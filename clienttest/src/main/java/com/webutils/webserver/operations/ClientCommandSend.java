package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.ObjectParams;
import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.manual.ClientInterface;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ClientCommandSend implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCommandSend.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_COMMAND_SEND;

    /*
     ** The overall controlling object that allocated the request context and threads.
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

    private final ObjectParams requestParams;

    private final ContentParser contentParser;

    private final MemoryManager memoryManager;

    /*
     ** This is to make the execute() function more manageable
     */
    private enum ExecutionState {
        SETUP_COMMAND_SEND_OPS,
        WAITING_FOR_CONN_COMP,
        SEND_CONTENT_DATA,
        WAITING_FOR_RESPONSE_HEADER,
        READ_RESPONSE_DATA,
        CONTENT_PROCESSED,
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
     ** The response buffer metering is used by the WritObjectToFile operation
     */
    private BufferManager responseBufferManager;
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
    ** The following is the default handler when the content is being displayed in a text format on the console.
     */
    public ClientCommandSend(final ClientInterface clientInterface, final ClientRequestContext requestContext,
                             final MemoryManager memoryManager, final ServerIdentifier server,
                             final ObjectParams requestParams) {
        this(clientInterface, requestContext, memoryManager,server, requestParams, null);
    }

    public ClientCommandSend(final ClientInterface clientInterface, final ClientRequestContext requestContext,
                           final MemoryManager memoryManager, final ServerIdentifier server,
                           final ObjectParams requestParams, final ContentParser contentParser) {

        this.clientInterface = clientInterface;
        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.objectServer = server;
        this.requestParams = requestParams;
        this.contentParser = contentParser;

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

        currState = ExecutionState.SETUP_COMMAND_SEND_OPS;

        String clientOpcRequestId = Objects.requireNonNullElse(requestParams.getOpcClientRequestId(), "");
        LOG.info("ClientCommandSend addr: " + objectServer.getServerIpAddress().toString() + " port: " +
                objectServer.getServerTcpPort() + " clientOpcRequestId: " + clientOpcRequestId);
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
            case SETUP_COMMAND_SEND_OPS:
                if (setupCommandSendOps()) {
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
                    currState = ExecutionState.SEND_CONTENT_DATA;
                    /*
                     ** Fall through to handle the case where the connection was setup and the response header was
                     **   received prior to this being scheduled.
                     */
                } else if ((status = requestContext.getStorageResponseResult(objectServer)) != HttpStatus.OK_200) {
                    LOG.warn("ClientCommandSend failure: " + status + " addr: " + objectServer.getServerIpAddress().toString() +
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

            case SEND_CONTENT_DATA:
                currState = ExecutionState.WAITING_FOR_RESPONSE_HEADER;
                teardownRequestWriteOps();
                event();
                break;

            case WAITING_FOR_RESPONSE_HEADER:
                if (httpInfo.getHeaderComplete()) {
                    int status = httpInfo.getResponseStatus();

                    LOG.info("WAITING_FOR_RESPONSE_HEADER status: " + status);

                    currState = ExecutionState.READ_RESPONSE_DATA;
                    /*
                     ** Fall through to the READ_RESPONSE_DATA state
                     */

                    if (status == -1) {
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

            case READ_RESPONSE_DATA:
                LOG.info("READ_RESPONSE_DATA");
                if (startContentRead()) {
                    currState = ExecutionState.CONTENT_PROCESSED;
                } else {
                    /*
                     ** Since startContentRead() returned false, that means the Content-Length was set to 0 and there
                     **   is nothing more to read.
                     */
                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                }
                break;

            case CONTENT_PROCESSED:
                boolean parseError = requestContext.getHttpParseError();

                /*
                ** getAllObjectDataWritten() returns the allObjectDataWritten flag that is set when either the content
                **   data has been processed by the ConvertRespBodyToString operation or the WriteObjectToFile operation
                **   has completed.
                ** Currently, the ParseContentBuffers operation uses the setPostContentParsed() method in the request
                **   context to inform higher levels that it is complete so that flag needs to be checked as well.
                **
                ** FIXME: Consolidate the flags
                 */
                boolean allDataWritten = requestContext.getAllObjectDataWritten();
                boolean contentParsed = requestContext.postMethodContentParsed();

                LOG.info("CONTENT_PROCESSED  allDataWritten: " + allDataWritten + " contentParsed: " + contentParsed +
                        " parseError: " + parseError);

                if (allDataWritten ||contentParsed) {
                    objectServer.setResponseStatus(HttpStatus.OK_200);

                    LOG.info("chunk read from Object Server complete");
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
                LOG.info("ClientCommandSend CALLBACK_OPS");

                /*
                 ** Display the results
                 */
                requestParams.outputResults(httpInfo);

                /*
                 ** Now call back the clientInterface to let the CLI command clean up
                 */
                currState = ExecutionState.EMPTY_STATE;
                clientInterface.clientRequestCompleted(httpInfo.getResponseStatus());
                break;

            case EMPTY_STATE:
                break;

            default:
                throw new IllegalStateException("Unexpected value: " + currState);
        }
    }

    /*
     ** This is called to cleanup the SetupChunkWrite and will tear down all the BufferManagerPointers and
     **   release the ByteBuffer(s) associated with the storageServerBufferManager.
     */
    public void complete() {

        LOG.info("ClientCommandSend complete() addr: " + objectServer.getServerIpAddress().toString() + " port: " +
                objectServer.getServerTcpPort());

        /*
        ** The following will tear down WRITE_TO_CLIENT and BUILD_REQUEST_HEADER if they have not already been
        **  taken care of.
         */
        teardownRequestWriteOps();

        /*
         ** The following must be called in order to make sure that the BufferManagerPointer
         **   dependencies are torn down in the correct order. The pointers in
         **   headerWriter are dependent upon the pointers in headerBuilder. But, if there was
         */
        Operation headerBuilder = requestHandlerOps.remove(OperationTypeEnum.BUILD_OBJECT_GET_HEADER);
        if (headerBuilder != null) {
            headerBuilder.complete();
        } else {
            Operation readFromFile = requestHandlerOps.remove(OperationTypeEnum.READ_OBJECT_FROM_FILE);
            if (readFromFile != null) {
                readFromFile.complete();
            }
        }

        Operation processResponse = requestHandlerOps.remove(OperationTypeEnum.RESPONSE_HANDLER);
        if (processResponse != null) {
            processResponse.complete();
        }

        /*
         ** Check if the ConvertRespBodyToString operation is present. It may not be in certain error conditions.
         */
        Operation convertBodyToStr = requestHandlerOps.remove(OperationTypeEnum.CONVERT_RESP_BODY_TO_STR);
        if (convertBodyToStr != null) {
            convertBodyToStr.complete();
        }

        /*
        ** Depending upon the response content, there is the possibility (assuming a content parser was provided) that
        **   one of the following operations was instantiated. In that case, they must be completed prior to the
        **   READ_BUFFER operation being completed due to the BufferPointer dependencies.
         */
        Operation parseContent = requestHandlerOps.remove(OperationTypeEnum.PARSE_CONTENT);
        if (parseContent != null) {
            parseContent.complete();
        }

        Operation parseErrorContent = requestHandlerOps.remove(OperationTypeEnum.PARSE_ERROR_CONTENT);
        if (parseErrorContent != null) {
            parseErrorContent.complete();
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
        //LOG.info("ClientCommandSend markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ClientCommandSend markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ClientCommandSend markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ClientCommandSend markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ClientCommandSend hasWaitTimeElapsed() not supposed to be on delayed queue");
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
     ** This sets up the operations and all of their dependencies to write the Object content data to the Object Server.
     **
     ** It will return false if the setup of the connection to the Object Server fails
     */
    private boolean setupCommandSendOps() {
        /*
         ** Allocate ByteBuffer(s) for the PUT request header that will be sent to the Object Server
         */
        FileReadBufferMetering bufferMetering = new FileReadBufferMetering(requestContext, memoryManager);
        requestHandlerOps.put(bufferMetering.getOperationType(), bufferMetering);
        BufferManagerPointer writeBufferPointer = bufferMetering.initialize();


        /*
         ** Allocate ByteBuffer(s) to read in the response from the Storage Server. By using a metering operation, the
         **   setup for the reading of the Object Server response header can be be deferred until the TCP connection to the
         **   Object Server is successful.
         */
        responseBufferMetering = new BufferReadMetering(requestContext, memoryManager);
        BufferManagerPointer respBufferPointer = responseBufferMetering.initialize();

        /*
         ** For each service, setup a HandleChunkWriteConnError operation that is used when there
         **   is an error communicating with the service.
         */
        HandleClientError errorHandler = new HandleClientError(requestContext, this);
        requestHandlerOps.put(errorHandler.getOperationType(), errorHandler);

        /*
         ** The Command Header must be written to the Object Server so that the data can be written following it
         */
        BuildRequestHeader headerBuilder = new BuildRequestHeader(requestContext, requestContext.getClientWriteBufferManager(),
                writeBufferPointer, requestParams);
        requestHandlerOps.put(headerBuilder.getOperationType(), headerBuilder);
        BufferManagerPointer writePointer = headerBuilder.initialize();

        WriteToClient headerWriter = new WriteToClient(requestContext, objectServerConn, this,
                writePointer, objectServer);
        requestHandlerOps.put(headerWriter.getOperationType(), headerWriter);
        headerWriter.initialize();

        /*
         ** For the Object Server, setup a ConnectComplete operation that is used when the NIO
         **   connection is made with the Object Server.
         */
        List<Operation> operationList = new LinkedList<>();
        operationList.add(headerBuilder);
        operationList.add(responseBufferMetering);
        ConnectComplete connectComplete = new ConnectComplete(requestContext, operationList, objectServer.getServerTcpPort());
        requestHandlerOps.put(connectComplete.getOperationType(), connectComplete);

        /*
         ** Use the ClientReadBufferManager to accept the response from the Object Server. This BufferManager is used
         **   to hold the response buffers as they are read in from the Object Server.
         **
         */
        responseBufferManager = requestContext.getClientReadBufferManager();

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
        if (!objectServerConn.startInitiator(objectServer.getServerIpAddress(), objectServer.getServerTcpPort(),
                connectComplete, errorHandler)) {
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
     ** This is used to tear down the request write operations after the request has been sent (either successfully or
     **   not).
     ** This is called from two different places so that the normal path can be handled and in complete(). If this is
     **   called via the complete() path and the request ran successfully, the two Operations will have already been
     **   removed from the map. So, need to check they are present prior to calling the complete() method for the
     **   operations.
     */
    private void teardownRequestWriteOps() {

        /*
         ** First tear down the BuildRequestHeader and WriteToClient operations. The WriteToClient needs to be
         **   removed since it's dependency will change to being on the ReadObjectFromFile operation instead of
         **   BuildRequestHeader.
         */
        Operation writeToClient = requestHandlerOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        if (writeToClient != null) {
            writeToClient.complete();
        }

        Operation buildPutHeader = requestHandlerOps.remove(OperationTypeEnum.BUILD_REQUEST_HEADER);
        if (buildPutHeader != null) {
            buildPutHeader.complete();
        }
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

        int contentLength = httpInfo.getContentLength();
        LOG.info("ClientCommandSend.startContentRead() contentLength: " + contentLength);

        if (contentLength != 0) {
            if (contentParser == null) {
                /*
                 ** The next Operations are run once the response header has been received. This is to convert the bytes
                 **   read in into a String.
                 */
                ConvertRespBodyToString convertToStr = new ConvertRespBodyToString(requestContext, httpBufferPointer,
                        responseBufferMetering, httpInfo, this);
                requestHandlerOps.put(convertToStr.getOperationType(), convertToStr);
                convertToStr.initialize();
            } else {
                Operation parseContent;
                if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
                    /*
                     ** The next Operations are run once the response header has been received. This is to convert the response
                     **   content into a usable form.
                     */
                    parseContent = new ParseContentBuffers(requestContext, responseBufferManager, httpBufferPointer,
                            responseBufferMetering, contentParser, contentLength, this);
                } else {
                    parseContent = new ParseErrorContent(requestContext, responseBufferManager, httpBufferPointer,
                            responseBufferMetering, contentLength, httpInfo, this);
                }
                requestHandlerOps.put(parseContent.getOperationType(), parseContent);
                parseContent.initialize();
            }

            return true;
        }

        LOG.info("startContentRead() Content-Length is 0");
        return false;
    }

}
