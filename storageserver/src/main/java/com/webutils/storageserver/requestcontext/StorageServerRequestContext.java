package com.webutils.storageserver.requestcontext;

import com.webutils.storageserver.operations.SetupStorageServerPut;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpMethodEnum;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.*;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;

public class StorageServerRequestContext extends RequestContext {

    /*
     **
     */
    private CloseOutRequest closeRequest;

    private DetermineRequestType determineRequestType;

    private SendFinalStatus sendFinalStatus;


    StorageServerRequestContext(final WebServerFlavor flavor, final MemoryManager memoryManager,
                                final EventPollThread threadThisRunsOn, final DbSetup dbSetup) {
        super(flavor, memoryManager, threadThisRunsOn, dbSetup);

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
     **          headers parsed callback from the HTTP Parser. Once all of the headers are parsed, the DetermineRequestType
     **          operations event() method is called.
     **          The final step for the ParseHttpRequest is to cleanup so that the RequestContext can be used again
     **          to parse another HTTP Request. This will allow a pool of RequestContext to be allocated at start of
     **          day and then reused.
     **
     ** The DetermineRequestType uses the information that the HTTP Parser generated and stored in the CasperHttpInfo
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

        readBuffer = new ReadBuffer(this, meteringPtr, clientConnection);
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
        sendFinalStatus = new SendFinalStatus(this, memoryManager, clientConnection);
        requestHandlerOperations.put(sendFinalStatus.getOperationType(), sendFinalStatus);
        BufferManagerPointer clientWritePtr = sendFinalStatus.initialize();

        closeRequest = new CloseOutRequest(this);
        requestHandlerOperations.put(closeRequest.getOperationType(), closeRequest);
        closeRequest.initialize();

        WriteToClient writeToClient = new WriteToClient(this, clientConnection,
                closeRequest, clientWritePtr);
        requestHandlerOperations.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        /*
         ** The DetermineRequestType operation is run after the HTTP Request has been parsed and the method
         **   handler determined via the setHttpMethodAndVersion() method in the CasperHttpInfo object.
         */
        determineRequestType = new DetermineRequestType(this, supportedHttpRequests);
        requestHandlerOperations.put(determineRequestType.getOperationType(), determineRequestType);
        determineRequestType.initialize();

        /*
         ** The HTTP Request methods that are supported are added to the supportedHttpRequests Map<> and are used
         **   by the DetermineRequestType operation to setup and run the appropriate handlers.
         */
        SetupStorageServerPut storageServerPutHandler = new SetupStorageServerPut(this, memoryManager, metering,
                determineRequestType);
        this.supportedHttpRequests.put(HttpMethodEnum.PUT_STORAGE_SERVER, storageServerPutHandler);

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
         **   DetermineRequestType. When the entire HTTP Request has been parsed, the ParseHttpRequest
         **   will event the DetermineRequestType operation to determine what operation sequence
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

        clientConnection.closeConnection();

        Operation operation;

        operation = requestHandlerOperations.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.SEND_FINAL_STATUS);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.REQUEST_FINISHED);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.READ_BUFFER);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.METER_READ_BUFFERS);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.DETERMINE_REQUEST_TYPE);
        operation.complete();

        /*
         ** Clear out the references to the Operations
         */
        metering = null;
        readBuffer = null;
        sendFinalStatus = null;
        closeRequest = null;
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
    }

}

