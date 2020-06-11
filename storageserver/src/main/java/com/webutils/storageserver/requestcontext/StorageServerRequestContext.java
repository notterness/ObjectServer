package com.webutils.storageserver.requestcontext;

import com.webutils.storageserver.operations.SetupStorageServerChunkDelete;
import com.webutils.storageserver.operations.SetupStorageServerGet;
import com.webutils.storageserver.operations.SetupStorageServerPut;
import com.webutils.storageserver.operations.StorageServerDetermineRequest;
import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpMethodEnum;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.*;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageServerRequestContext extends RequestContext {

    private static final Logger LOG = LoggerFactory.getLogger(StorageServerRequestContext.class);

    /*
     **
     */
    private StorageServerDetermineRequest determineRequestType;


    StorageServerRequestContext(final MemoryManager memoryManager, final HttpRequestInfo httpInfo, final EventPollThread threadThisRunsOn,
                                final ServerIdentifierTableMgr serverTableMgr, final int threadId, final WebServerFlavor flavor) {

        super(memoryManager, httpInfo, threadThisRunsOn, serverTableMgr, threadId, flavor);
    }

    /*
     ** This sets up the RequestContext to handle server requests. When it is operating as a
     **   server, the first thing is expects is an HTTP request to arrive. This should be in
     **   the first one or two buffers read in from the connection.
     **
     ** The reading and parsing of the HTTP request is handled by the following operations:
     **   -> BufferReadMetering - This hands out pre-allocated ByteBuffers to the ClientReadBufferManager.
     **   -> ReadBuffer - This informs the IoInterface that there are ByteBuffers ready to have data read into them.
     **          ReadBuffer has a BufferManagerPointer dependency on buffers that are made available by the
     **          BufferReadMetering operation.
     **   -> ParseHttpRequest - This has a dependency on the ClientReadBufferManager and the ReadBuffer BufferManagerPointer.
     **          When the data is read into the ByteBuffer by the IoInterface, it will call the ParseHttpRequest event()
     **          method. This will queue up the operation to be handled by the EventPollThread. When the execute()
     **          method for ParseHttpRequest is called, it will parse all available buffers until it receives the all
     **          headers parsed callback from the HTTP Parser. Once all of the headers are parsed, the DetermineRequest
     **          operations event() method is called.
     **          The final step for the ParseHttpRequest is to cleanup so that the RequestContext can be used again
     **          to parse another HTTP Request. This will allow a pool of RequestContext to be allocated at start of
     **          day and then reused.
     **
     ** The DetermineRequest uses the information that the HTTP Parser generated and stored in the HttpInfo
     **   object to setup the correct method handler. There is a list of supported HTTP Methods kept in the
     **   Map<Operation> supportedHttpRequests. Once the correct request is determined, the Operation to setup the
     **   method handler is run.
     */
    public void initializeServer(final IoInterface connection, final int requestId) {
        clientConnection = connection;
        connectionRequestId = requestId;

        /*
         ** Setup the Metering and Read pointers since they are required for the HTTP Parser and for most
         **   HTTP Request handlers.
         */
        metering = new BufferReadMetering(this, memoryManager);
        requestHandlerOperations.put(metering.getOperationType(), metering);
        BufferManagerPointer meteringPtr = metering.initialize();

        ReadBuffer readBuffer = new ReadBuffer(this, clientReadBufferManager, meteringPtr, clientConnection);
        requestHandlerOperations.put(readBuffer.getOperationType(), readBuffer);
        readPointer = readBuffer.initialize();


        /*
         ** The DetermineRequest operation is run after the HTTP Request has been parsed and the method
         **   handler determined via the setHttpMethodAndVersion() method in the HttpInfo object.
         */
        determineRequestType = new StorageServerDetermineRequest(this, supportedHttpRequests, memoryManager,
                clientConnection);
        requestHandlerOperations.put(determineRequestType.getOperationType(), determineRequestType);
        determineRequestType.initialize();

        /*
         ** The HTTP Request methods that are supported are added to the supportedHttpRequests Map<> and are used
         **   by the DetermineRequest operation to setup and run the appropriate handlers.
         **
         ** NOTE: Although it seems weird to add the supported HTTP requests after the creating of the
         **   StorageServerDetermineRequest, the method handler have a dependency upon the determine request.
         */
        SetupStorageServerPut storageServerPutHandler = new SetupStorageServerPut(this, metering, determineRequestType);
        this.supportedHttpRequests.put(HttpMethodEnum.PUT_METHOD, storageServerPutHandler);

        SetupStorageServerGet storageServerGetHandler = new SetupStorageServerGet(this, memoryManager, determineRequestType);
        this.supportedHttpRequests.put(HttpMethodEnum.GET_METHOD, storageServerGetHandler);

        SetupStorageServerChunkDelete storageServerDeleteHandler = new SetupStorageServerChunkDelete(this, determineRequestType);
        this.supportedHttpRequests.put(HttpMethodEnum.DELETE_METHOD, storageServerDeleteHandler);

        /*
         ** Setup the specific part for parsing the buffers as an HTTP Request.
         */
        initializeHttpParsing();
    }

    /*
     ** This is used to setup the common HTTP Request parsing Operations and their dependencies
     */
    protected void initializeHttpParsing() {
        /*
         **
         */
        super.initializeHttpParsing();

        /*
         ** The two Operations that run to perform the HTTP Parsing are ParseHttpRequest and
         **   DetermineRequest. When the entire HTTP Request has been parsed, the ParseHttpRequest
         **   will event the DetermineRequest operation to determine what operation sequence
         **   to setup.
         */
        ParseHttpRequest httpParser = new ParseHttpRequest(this, readPointer, metering, determineRequestType);
        requestHandlerOperations.put(httpParser.getOperationType(), httpParser);
        httpParser.initialize();

        /*
         ** Now Meter out a buffer to read in the HTTP request
         */
        metering.event();
    }

    /*
     ** This is used to cleanup the HTTP Request parsing Operations and their dependencies
     */
    public void cleanupHttpParser() {
        Operation operation;

        operation = requestHandlerOperations.remove(OperationTypeEnum.PARSE_HTTP_BUFFER);
        operation.complete();
    }

    /*
     ** This is called when the request has completed and this RequestContext needs to be put back into
     **   a pristine state so it can be used for a new request.
     ** Once it is cleaned up, this RequestContext is added back to the free list so it can be used again.
     */
    public void cleanupServerRequest() {

        LOG.info("StorageServerRequestContext cleanupServerRequest()");

        if (clientConnection != null) {
            clientConnection.closeConnection();

            Operation operation;

            operation = requestHandlerOperations.remove(OperationTypeEnum.READ_BUFFER);
            operation.complete();

            operation = requestHandlerOperations.remove(OperationTypeEnum.METER_READ_BUFFERS);
            operation.complete();

            operation = requestHandlerOperations.remove(OperationTypeEnum.STORAGE_SERVER_DETERMINE_REQUEST);
            operation.complete();

            /*
             ** Remove the supported HTTP request types
             */
            supportedHttpRequests.clear();

            /*
            ** Remove any remaining request handler operations
             */
            requestHandlerOperations.clear();

            /*
             ** Clear out the references to the Operations
             */
            metering = null;
            determineRequestType = null;

            /*
             ** Call reset() to make sure the BufferManager(s) have released all the references to
             **   ByteBuffer(s).
             */
            reset();

            /*
             ** Finally release the clientConnection back to the free pool.
             */
            releaseConnection(clientConnection);
            clientConnection = null;

            /*
             ** Add the context back to the free pool
             */
            releaseContext();
        }
    }

    /*
     ** The following are stubs until I sort out how the RequestContext and RequestContext pool objects should be
     **   properly handled.
     */
    public boolean hasHttpRequestBeenSent(final ServerIdentifier storageServerId) {
        LOG.error("Invalid function");
        return false;
    }

    public void setHttpRequestSent(final ServerIdentifier storageServerId) {
        LOG.error("Invalid function");
    }

    public void removeHttpRequestSent(final ServerIdentifier storageServerId) {
        LOG.error("Invalid function");
    }

    public boolean hasStorageServerResponseArrived(final ServerIdentifier storageServerId) {
        LOG.error("Invalid function");
        return false;
    }

    public int getStorageResponseResult(final ServerIdentifier storageServerId) {
        LOG.error("Invalid function");
        return -1;
    }

    public void setStorageServerResponse(final ServerIdentifier storageServerId, final int result) {
        LOG.error("Invalid function");
    }

    public void removeStorageServerResponse(final ServerIdentifier storageServerId) {
        LOG.error("Invalid function");
    }

    public BufferManager getStorageServerWriteBufferManager() {
        LOG.error("Invalid function");
        return null;
    }


}

