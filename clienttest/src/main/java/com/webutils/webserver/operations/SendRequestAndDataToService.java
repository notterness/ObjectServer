package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.ObjectParamsWithData;
import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SendRequestAndDataToService implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SendRequestAndDataToService.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final static OperationTypeEnum operationType = OperationTypeEnum.SEND_SERVICE_REQUEST_PLUS_DATA;

    private final static int REMOTE_READ_BUFFER_MANAGER = 4000;
    private final static int REMOTE_WRITE_BUFFER_MANAGER = 4100;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The ServerIdentifier is this object reader's unique identifier. It identifies the Object Server through the
     **   IP address and Port number.
     */
    private final ServerIdentifier service;

    private final ObjectParamsWithData requestParams;

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
     ** The response buffer metering is used by the
     */
    private BufferManager responseBufferManager;
    private ServiceResponseBufferMetering responseBufferMetering;

    private BufferManagerPointer httpBufferPointer;

    private BufferManager serviceWriteBufferMgr;

    /*
     ** The HttpResponseInfo is unique to this read chunk operation as the response coming back is only for one chunk
     **   worth of data.
     */
    private final HttpResponseInfo httpInfo;

    private final ContentParser contentParser;

    /*
     ** The operation that is called when this has completed
     */
    private final Operation completeCallback;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> requestHandlerOps;

    /*
     ** The following is the connection used to communicate with the Storage Server
     */
    private final IoInterface objectServerConn;

    /*
     ** NOTE: This call the server.setHttpInfo() must be made before instantiating the SendRequestToService class.
     */
    public SendRequestAndDataToService(final RequestContext requestContext, final MemoryManager memoryManager,
                                       final ServerIdentifier server, final ObjectParamsWithData commandParams,
                                       final ContentParser contentParser, final Operation completeCb) {

        this.requestContext = requestContext;
        this.service = server;
        this.requestParams = commandParams;
        this.memoryManager = memoryManager;

        this.contentParser = contentParser;

        this.completeCallback = completeCb;

        /*
         ** Obtain the HttpResponseInfo for this request.
         */
        this.httpInfo = service.getHttpInfo();

        this.objectServerConn = requestContext.allocateConnection(this);

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        requestHandlerOps = new HashMap<>();

        currState = ExecutionState.SETUP_COMMAND_SEND_OPS;

        LOG.info("SendRequestToService addr: " + service.getServerIpAddress().toString() + " port: " +
                service.getServerTcpPort());
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
     ** The execute() method consists of a state machine to perform the following:
     **
     **   SETUP_COMMAND_SEND_OPS - This sets up the operations needed to send the request to the remote service and the
     **     operations needed to read back in the response headers and content. Once the operations are all setup it
     **     attempts to make the connection to the remote service.
     **   WAITING_FOR_CONN_COMP - This is the waiting state for the request to be sent to the remote service.
     **   SEND_CONTENT_DATA - This cleans up some of the operations that were used to send the request to the remote
     **     service. It is a placeholder for sending content data, but currently that is not supported.
     **   WAITING_FOR_RESPONSE_HEADER - This is where the state machine waits for the response header(s) from the
     **     remote service. When this state completes, all of the response header(s) will have been read in and added
     **     to the HTTP information structure and the next step is to read in the content data (if the "Content-Length"
     **     is not set to 0).
     **   READ_RESPONSE_DATA - Read the content data and parse it.
     **   CONTENT_PROCESSED - All the data from the remote service has been read in and this request is completed.
     **   CALLBACK_OPS - Callback the initiating operation.
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
                    service.setResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
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
                if (requestContext.hasHttpRequestBeenSent(service)) {

                    requestContext.removeHttpRequestSent(service);
                    currState = ExecutionState.SEND_CONTENT_DATA;
                    /*
                     ** Fall through to handle the case where the connection was setup and the response header was
                     **   received prior to this being scheduled.
                     */
                } else if ((status = requestContext.getStorageResponseResult(service)) != HttpStatus.OK_200) {
                    LOG.warn("SendRequestToService failure: " + status + " addr: " + service.getServerIpAddress().toString() +
                            " port: " + service.getServerTcpPort());

                    service.setResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);

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
                setupContentWrite();
                event();
                break;

            case WAITING_FOR_RESPONSE_HEADER:
                if (httpInfo.getHeaderComplete()) {
                    int status = httpInfo.getResponseStatus();

                    LOG.info("WAITING_FOR_RESPONSE_HEADER status: " + status + " contentLength: " + httpInfo.getContentLength());

                    service.setResponseStatus(status);

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
                LOG.info("CONTENT_PROCESSED  parseError: " + parseError);

                if (parseError) {
                    /*
                     ** Need to callback the higher level
                     */
                    LOG.info("SendRequestToService parser error");
                }
                /*
                 ** Fall through
                 */
                currState = ExecutionState.CALLBACK_OPS;

            case CALLBACK_OPS:
                LOG.info("ClientCommandSend CALLBACK_OPS");

                /*
                 ** Display the results
                 */
                requestParams.outputResults(httpInfo);

                /*
                 ** Now call back the operation that made the request to the remote service
                 */
                completeCallback.event();

                currState = ExecutionState.EMPTY_STATE;
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

        LOG.info("SendRequestToService complete() addr: " + service.getServerIpAddress().toString() + " port: " +
                service.getServerTcpPort());

        /*
         ** The following must be called in order to make sure that the BufferManagerPointer
         **   dependencies are torn down in the correct order. The pointers in
         **   headerWriter are dependent upon the pointers in headerBuilder. But, if there was
         */
        Operation headerWriter = requestHandlerOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        if (headerWriter != null) {
            headerWriter.complete();
        }

        Operation requestBuilder = requestHandlerOps.remove(OperationTypeEnum.BUILD_HEADERS_AND_DATA);
        if (requestBuilder != null) {
            requestBuilder.complete();
        }

        Operation processResponse = requestHandlerOps.remove(OperationTypeEnum.SERVICE_RESPONSE_HANDLER);
        if (processResponse != null) {
            processResponse.complete();
        }

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
         **   from the remote service
         */
        responseBufferMetering.complete();
        responseBufferManager = null;

        /*
         ** Clear the HTTP Request sent for this Storage Server
         */
        requestContext.removeHttpRequestSent(service);

        /*
         ** remove the HttpResponseInfo association from the ServerIdentifier
         */
        service.setHttpInfo(null);
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
        //LOG.info("SendRequestToService markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SendRequestToService markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SendRequestToService markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SendRequestToService markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SendRequestToService hasWaitTimeElapsed() not supposed to be on delayed queue");
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
         ** Allocate ByteBuffer(s) for the request header that will be sent to the remote service
         */
        serviceWriteBufferMgr = new BufferManager(SendRequestToService.REMOTE_SERVICE_RESPONSE_BUFFERS, "ServiceWriteBufferMgr",
                REMOTE_WRITE_BUFFER_MANAGER);

        BufferWriteMetering bufferMetering = new BufferWriteMetering(requestContext, memoryManager, serviceWriteBufferMgr);
        requestHandlerOps.put(bufferMetering.getOperationType(), bufferMetering);
        BufferManagerPointer writeBufferPointer = bufferMetering.initialize();


        /*
         ** Create a BufferManager to accept the response from the remote service. This BufferManager is just used
         **   as a placeholder for buffers while they are processed.
         **
         */
        responseBufferManager = new BufferManager(SendRequestToService.REMOTE_SERVICE_RESPONSE_BUFFERS, "ServiceReadBufferMgr",
                REMOTE_READ_BUFFER_MANAGER);

        /*
         ** Allocate ByteBuffer(s) to read in the response from the remote service. By using a metering operation, the
         **   setup for the reading of the Object Server response header can be be deferred until the TCP connection to the
         **   Object Server is successful.
         */
        responseBufferMetering = new ServiceResponseBufferMetering(requestContext, memoryManager, responseBufferManager,
                SendRequestToService.REMOTE_SERVICE_RESPONSE_BUFFERS);
        BufferManagerPointer respBufferPointer = responseBufferMetering.initialize();

        /*
         ** For each Storage Server, setup a HandleChunkWriteConnError operation that is used when there
         **   is an error communicating with the StorageServer.
         */
        HandleClientError errorHandler = new HandleClientError(requestContext, this);
        requestHandlerOps.put(errorHandler.getOperationType(), errorHandler);

        /*
         ** BuildHeadersAndData creates both the Request Headers and the data that follows it. It feeds the buffers into
         **   the BufferManager that is being used by the WriteToClient to send the data to the remote service.
         */
        BuildHeadersAndData requestBuilder = new BuildHeadersAndData(requestContext, serviceWriteBufferMgr,
                writeBufferPointer, bufferMetering, requestParams);
        requestHandlerOps.put(requestBuilder.getOperationType(), requestBuilder);
        BufferManagerPointer writePointer = requestBuilder.initialize();

        WriteToClient headerWriter = new WriteToClient(requestContext, objectServerConn, this,
                serviceWriteBufferMgr, writePointer, service);
        requestHandlerOps.put(headerWriter.getOperationType(), headerWriter);
        headerWriter.initialize();

        /*
         ** For the remote service, setup a ConnectComplete operation that is used when the NIO
         **   connection is made with the remote service server.
         */
        List<Operation> operationList = new LinkedList<>();
        operationList.add(requestBuilder);
        operationList.add(responseBufferMetering);
        ConnectComplete connectComplete = new ConnectComplete(requestContext, operationList, service.getServerTcpPort());
        requestHandlerOps.put(connectComplete.getOperationType(), connectComplete);

        /*
         ** Setup the operations to read in the HTTP Response header and process it
         */
        ReadBuffer readBuffer = new ReadBuffer(requestContext, responseBufferManager, respBufferPointer, objectServerConn);
        requestHandlerOps.put(readBuffer.getOperationType(), readBuffer);
        httpBufferPointer = readBuffer.initialize();


        ServiceResponseHandler httpRespHandler = new ServiceResponseHandler(requestContext, responseBufferManager, httpBufferPointer,
                responseBufferMetering, this, service);
        requestHandlerOps.put(httpRespHandler.getOperationType(), httpRespHandler);
        httpRespHandler.initialize();

        /*
         ** Now open a initiator connection to write encrypted buffers out of.
         */
        if (!objectServerConn.startInitiator(service.getServerIpAddress(), service.getServerTcpPort(),
                connectComplete, errorHandler)) {
            /*
             ** This means the SocketChannel could not be opened. Need to indicate a problem
             **   with the Storage Server and clean up this SetupChunkWrite.
             ** Set the error to indicate that the Storage Server cannot be reached.
             **
             ** TODO: Add a test case for the startInitiator failing to make sure the cleanup
             **   is properly handled.
             */
            return false;
        }

        return true;
    }

    /*
     ** This is used to setup the write of content data to the service
     */
    private void setupContentWrite() {

        /*
         ** First tear down the BuildHeadersAndData and WriteToClient operations. The WriteToClient needs to be
         **   removed since it's dependency will change to being on a passed in buffer instead of
         **   BuildRequestHeader.
         */
        Operation writeToClient = requestHandlerOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        writeToClient.complete();

        Operation requestBuilder = requestHandlerOps.remove(OperationTypeEnum.BUILD_HEADERS_AND_DATA);
        requestBuilder.complete();

        Operation writeMetering = requestHandlerOps.remove(OperationTypeEnum.BUFFER_WRITE_METERING);
        writeMetering.complete();
    }


    /*
     ** This is used to read in the content returned with the response headers. If the Content-Length is 0, it will not
     **   setup anything and will simply return false.
     */
    private boolean startContentRead() {
        /*
         ** The following must be called in order to make sure that the BufferManagerPointer dependencies are torn down
         **   in the correct order. The pointers in httpRespHandler are dependent upon the pointers in readRespBuffer.
         */
        Operation httpRespHandler = requestHandlerOps.remove(OperationTypeEnum.SERVICE_RESPONSE_HANDLER);
        httpRespHandler.complete();

        int contentLength = httpInfo.getContentLength();
        if (contentLength != 0) {
            /*
             ** The handling is different for error responses versus a good response. An error response generally has
             **   the form of:
             **    {
             **       "code": Integer value of error
             **       "message": "the error message"
             **       --> then optional additional data in the form of the following and there may be multiple entries
             **       "field name": "field information"
             **    }
             */
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

            return true;
        }

        LOG.info("startContentRead() Content-Length is 0");
        return false;
    }

}
