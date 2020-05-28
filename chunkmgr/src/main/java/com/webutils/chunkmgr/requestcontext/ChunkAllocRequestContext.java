package com.webutils.chunkmgr.requestcontext;

import com.webutils.chunkmgr.operations.*;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChunkAllocRequestContext extends RequestContext {


    private static final Logger LOG = LoggerFactory.getLogger(ChunkAllocRequestContext.class);

    /*
     ** This is the BufferManager used to hold the data being written out the Storage Servers. Since the same data is
     **   being written to all Storage Servers (at this point), there is a one to many relationship.
     */
    protected final BufferManager storageServerWriteBufferManager;

    /*
     **
     */
    private CloseOutRequest closeRequest;

    private ChunkMgrDetermineRequest determineRequest;

    private ChunkMgrSendFinalStatus sendFinalStatus;

    /*
     ** This is the unique ID to identify an object table entry in the database (ObjectStorageDb, object table)
     */
    private int objectId;

    /*
     ** The following Map is used to keep track of when the HTTP Request is sent to the
     **   Storage Server from the Web Server and it is used by the test code to know that
     **   the HTTP Request has been sent by the client to the Web Server.
     ** The map is based upon the IP address and the TCP Port of the target plus the chunk number.
     */
    private final Map<ServerIdentifier, AtomicBoolean> httpRequestSent;

    /*
     ** The following Map is used to indicate that a Storage Server has responded.
     */
    private final Map<ServerIdentifier, Integer> storageServerResponse;


    public ChunkAllocRequestContext(final MemoryManager memoryManager, final HttpRequestInfo httpInfo,
                                    final EventPollThread threadThisRunsOn, final ServerIdentifierTableMgr serverTableMgr,
                                    final int threadId, final WebServerFlavor flavor) {

        super(memoryManager, httpInfo, threadThisRunsOn, serverTableMgr, threadId, flavor);

        /*
         ** The BufferManager(s) that are allocated here are populated in the following Operations:
         **   clientReadBufferManager - This is populated with ByteBuffer(s) in the BufferReadMetering operation
         **   clientWriteBufferManager - This is populated in the SendFinalStatus operation, but will need to be changed
         **     once the GET request is implemented to allowing streaming of data back to the clients.
         **   storageServerWriteBufferManager - This is populated in the EncryptBuffer operation.
         */
        int bufferMgrRingSize = memoryManager.getBufferManagerRingSize();
        this.storageServerWriteBufferManager = new BufferManager(bufferMgrRingSize, "StorageServerWrite", 300);


        this.storageServerResponse = new HashMap<>();

        /*
         ** Setup the map for the HTTP Request Sent
         */
        this.httpRequestSent = new HashMap<>();

        this.objectId = -1;
    }

    /*
     ** This is used to clean up both the Client and Server side for the RequestContext.
     */
    public void reset() {

        super.reset();

        /*
         ** Now reset the BufferManagers back to their pristine state. That means that there are no
         **   registered BufferManagerPointers or dependencies remaining associated with the
         **   BufferManager.
         */
        clientReadBufferManager.reset();
        clientWriteBufferManager.reset();
        storageServerWriteBufferManager.reset();

        /*
         ** Clear out the Map<> associated with HTTP Requests and Responses from the Storage Server and then
         **   the Map<> that keeps track if an HTTP Request has been sent (used for the client side).
         */
        storageServerResponse.clear();
        httpRequestSent.clear();

        objectId = -1;
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
     ** The DetermineRequest uses the information that the HTTP Parser generated and stored in the CasperHttpInfo
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
         ** The SendFinalStatus, WriteToClient and CloseOutRequest are tied together. The SendFinalStatus is
         **   responsible for building the final HTTP status response to the client. The WriteToClient is
         **   responsible for kicking the IoInterface to write the data out and waiting for the data
         **   pointer to be updated to know that the data has been written. Once the data has been all written, then
         **   the WriteToClient operation will event() the CloseOutRequest operation.
         **   The CloseOutRequest is executed after the write to the client completes and it is responsible for
         **   cleaning up the Request and its associated connection.
         **   Once the cleanup is performed, then the RequestContext is added back to the free list so
         **   it can be used to handle a new request.
         */
        sendFinalStatus = new ChunkMgrSendFinalStatus(this, memoryManager);
        requestHandlerOperations.put(sendFinalStatus.getOperationType(), sendFinalStatus);
        BufferManagerPointer clientWritePtr = sendFinalStatus.initialize();

        closeRequest = new CloseOutRequest(this);
        requestHandlerOperations.put(closeRequest.getOperationType(), closeRequest);
        closeRequest.initialize();

        WriteToClient writeToClient = new WriteToClient(this, clientConnection,
                closeRequest, clientWritePtr, null);
        requestHandlerOperations.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        /*
         ** The DetermineRequest operation is run after the HTTP Request has been parsed and the method
         **   handler determined via the setHttpMethodAndVersion() method in the CasperHttpInfo object.
         */
        determineRequest = new ChunkMgrDetermineRequest(this, supportedHttpRequests);
        requestHandlerOperations.put(determineRequest.getOperationType(), determineRequest);
        determineRequest.initialize();

        /*
         ** The HTTP Request methods that are supported are added to the supportedHttpRequests Map<> and are used
         **   by the DetermineRequest operation to setup and run the appropriate handlers.
         **
         ** NOTE: Although it seems weird to add the supported HTTP requests after the creating of the
         **   DetermineRequest, the method handler have a dependency upon the determine request.
         */
        SetupCreateServerPost createServerHandler = new SetupCreateServerPost(this, metering, determineRequest);
        supportedHttpRequests.put(HttpMethodEnum.POST_METHOD, createServerHandler);

        SetupAllocateChunksGet allocateChunksHandler = new SetupAllocateChunksGet(this, metering, determineRequest);
        supportedHttpRequests.put(HttpMethodEnum.GET_METHOD, allocateChunksHandler);

        SetupListChunksGet listChunksHandler = new SetupListChunksGet(this, memoryManager, determineRequest);
        supportedHttpRequests.put(HttpMethodEnum.LIST_CHUNKS_METHOD, listChunksHandler);

        SetupListServersGet listServersHandler = new SetupListServersGet(this, memoryManager, determineRequest);
        supportedHttpRequests.put(HttpMethodEnum.LIST_SERVERS_METHOD, listServersHandler);

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
        ParseHttpRequest httpParser = new ParseHttpRequest(this, readPointer, metering, determineRequest);
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

        clientConnection.closeConnection();

        Operation operation;

        operation = requestHandlerOperations.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.CHUNK_MGR_SEND_FINAL_STATUS);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.CLOSE_OUT_REQUEST);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.READ_BUFFER);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.METER_READ_BUFFERS);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.CHUNK_MGR_DETERMINE_REQUEST);
        operation.complete();

        /*
         ** Clear out the supported operations Map<>
         */
        supportedHttpRequests.clear();

        /*
         ** Clear out the references to the Operations
         */
        metering = null;
        sendFinalStatus = null;
        closeRequest = null;
        determineRequest = null;

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

    /*
     ** The following are used to keep track of Storage Servers and if the HTTP Request has been sent successfully
     **   to it. The setter (setHttpResponseSent() is called by ClientHttpRequestWite after the buffer has been
     **   written to the SocketChannel.
     */
    public boolean hasHttpRequestBeenSent(final ServerIdentifier storageServerId) {
        AtomicBoolean responseSent = httpRequestSent.get(storageServerId);
        if (responseSent != null) {
            return responseSent.get();
        }
        return false;
    }

    public void setHttpRequestSent(final ServerIdentifier storageServerId) {
        AtomicBoolean httpSent = new AtomicBoolean(true);
        httpRequestSent.put(storageServerId, httpSent);
    }

    /*
     ** This is used to remove the Map<> entry for the Storage Server after it has completed it's writes
     */
    public void removeHttpRequestSent(final ServerIdentifier storageServerId) {
        if (httpRequestSent.remove(storageServerId) == null) {
            LOG.warn("RequestContext[" + getRequestId() + "] HTTP Request remove failed targetPort: " +
                    storageServerId.getServerIpAddress() + ":" + storageServerId.getServerTcpPort() +
                    ":" + storageServerId.getChunkNumber());

        }
    }

    /*
     ** The following are used to keep track of the HTTP Response from the Storage Server.
     */
    public boolean hasStorageServerResponseArrived(final ServerIdentifier storageServerId) {
        Integer responseSent = storageServerResponse.get(storageServerId);
        return (responseSent != null);
    }

    public int getStorageResponseResult(final ServerIdentifier storageServerId) {
        Integer responseSent = storageServerResponse.get(storageServerId);
        if (responseSent != null) {
            return responseSent;
        }
        return -1;
    }

    public void setStorageServerResponse(final ServerIdentifier storageServerId, final int result) {
        Integer storageServerResult = result;
        storageServerResponse.put(storageServerId, storageServerResult);
    }

    public void removeStorageServerResponse(final ServerIdentifier storageServerId) {
        if (storageServerResponse.remove(storageServerId) == null) {
            LOG.warn("RequestContext[" + getRequestId() + "] HTTP Response remove failed targetPort: " +
                    storageServerId.getServerIpAddress() + ":" + storageServerId.getServerTcpPort() +
                    ":" + storageServerId.getChunkNumber());
        }
    }

    public BufferManager getStorageServerWriteBufferManager() {
        return storageServerWriteBufferManager;
    }

    public void setObjectId(final int id) { objectId = id;}

    public int getObjectId() { return objectId; }

}
