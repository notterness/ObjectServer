package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.*;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class SetupChunkWrite implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupChunkWrite.class);

    private final int STORAGE_SERVER_HEADER_BUFFER_COUNT = 4;

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
    ** The StorageServerIdentifer is this chunk write's unique identifier. It is determined through the VON
    **   checker. It identifies the chunk and the Storage Server through the  IP address, Port number and
    **   the chunk number.
     */
    private final ServerIdentifier storageServer;

    private final MemoryManager memoryManager;

    private final Operation completeCallback;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    private final BufferManagerPointer encryptedBufferPtr;

    private final int chunkBytesToEncrypt;

    /*
    ** An integer that identifies which of the Storage Server writers this is
     */
    private final int writerNumber;

    /*
    ** The following is set to null in normal cases or it is set to a value when the ChunkWrite want the target Storage
    **   Server to respond with an error or to close the TCP connection at certain times during the transfer.
     */
    private final String errorInjectString;

    private BufferManager storageServerBufferManager;
    private BufferManager storageServerResponseBufferManager;

    private BufferManagerPointer addBufferPointer;

    private StorageServerResponseBufferMetering responseBufferMetering;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> requestHandlerOperations;

    /*
    ** The following is the connection used to communicate with the Storage Server
     */
    private IoInterface storageServerConnection;

    private boolean chunkWriteSetupComplete;

    private boolean serverConnectionClosedDueToError;

    /*
    ** SetupChunkWrite is called at the beginning of each chunk (128MB) block of data. This is what sets
    **   up the calls to obtain the VON information and the meta-data write to the database.
     */
    public SetupChunkWrite(final RequestContext requestContext, final ServerIdentifier server,
                           final MemoryManager memoryManager, final BufferManagerPointer encryptedBufferPtr,
                           final int chunkBytesToEncrypt, final Operation completeCb, final int writer,
                           final String errorInjectString) {

        this.operationType = OperationTypeEnum.fromInt(OperationTypeEnum.SETUP_CHUNK_WRITE_0.toInt() + writer);
        this.requestContext = requestContext;
        this.storageServer = server;
        this.memoryManager = memoryManager;
        this.encryptedBufferPtr = encryptedBufferPtr;
        this.chunkBytesToEncrypt = chunkBytesToEncrypt;

        this.completeCallback = completeCb;

        this.writerNumber = writer;

        this.errorInjectString = errorInjectString;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        requestHandlerOperations = new HashMap<>();

        chunkWriteSetupComplete = false;

        serverConnectionClosedDueToError = false;

        LOG.info("SetupChunkWrite[" + requestContext.getRequestId() + "] addr: " +
                storageServer.getServerIpAddress().toString() + " port: " +
                storageServer.getServerTcpPort() + " chunkNumber: " + storageServer.getChunkNumber() + " offset: " +
                storageServer.getOffset() + " writer: " + this.writerNumber);
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
     **
     */
    public void execute() {
        if (!chunkWriteSetupComplete) {
            BufferManagerPointer respBufferPointer;

            chunkWriteSetupComplete = true;

            /*
             ** Create a BufferManager with two required entries to send the HTTP Request header to the
             **   Storage Server.
             */
            storageServerBufferManager = new BufferManager(STORAGE_SERVER_HEADER_BUFFER_COUNT,
                    "StorageServer", 1000 + (writerNumber * 10));

            /*
             ** Create a BufferManager to accept the HTTP Response from the Storage Server
             */
            storageServerResponseBufferManager = new BufferManager(STORAGE_SERVER_HEADER_BUFFER_COUNT,
                    "StorageServerResponse", 1100 + (writerNumber * 10));

            /*
             ** Allocate ByteBuffer(s) for the header
             */
            addBufferPointer = storageServerBufferManager.register(this);
            storageServerBufferManager.bookmark(addBufferPointer);
            for (int i = 0; i < STORAGE_SERVER_HEADER_BUFFER_COUNT; i++) {
                ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, storageServerBufferManager,
                        operationType);

                storageServerBufferManager.offer(addBufferPointer, buffer);
            }

            storageServerBufferManager.reset(addBufferPointer);

            /*
             ** Allocate ByteBuffer(s) to read in the HTTP Response from the Storage Server. By using a metering operation, the
             **   setup for the reading of the Storage Server response header can be be deferred until the TCP connection to the
             **   Storage Server is successful.
             */
            responseBufferMetering = new StorageServerResponseBufferMetering(requestContext, memoryManager, storageServerResponseBufferManager,
                    STORAGE_SERVER_HEADER_BUFFER_COUNT);
            respBufferPointer = responseBufferMetering.initialize();

            /*
             ** For each Storage Server, setup a HandleChunkWriteConnError operation that is used when there
             **   is an error communicating with the StorageServer.
             */
            HandleChunkWriteConnError errorHandler = new HandleChunkWriteConnError(requestContext, this, storageServer);
            requestHandlerOperations.put(errorHandler.getOperationType(), errorHandler);

            /*
             ** For each Storage Server, create the connection used to communicate with it.
             */
            storageServerConnection = requestContext.allocateConnection(this);

            /*
             ** For each Storage Server, create a WriteToStorageServer operation that will handle writing the data out
             **   to the Storage Server. The WriteToStorageServer will use the bookmark created in the
             **   EncryptBuffer operation to know where to start writing the data.
             */
            WriteToStorageServer storageServerWriter = new WriteToStorageServer(requestContext, storageServerConnection,
                    encryptedBufferPtr, chunkBytesToEncrypt, storageServer);
            requestHandlerOperations.put(storageServerWriter.getOperationType(), storageServerWriter);
            storageServerWriter.initialize();

            /*
             ** The PUT Header must be written to the Storage Server prior to sending the data
             */
            BuildHeaderToStorageServer headerBuilder = new BuildHeaderToStorageServer(requestContext, storageServerBufferManager,
                    addBufferPointer, storageServer, errorInjectString);
            requestHandlerOperations.put(headerBuilder.getOperationType(), headerBuilder);
            BufferManagerPointer writePointer = headerBuilder.initialize();

            List<Operation> ops = new LinkedList<>();
            ops.add(storageServerWriter);
            WriteHeaderToStorageServer headerWriter = new WriteHeaderToStorageServer(requestContext, storageServerConnection, ops,
                    storageServerBufferManager, writePointer, storageServer);
            requestHandlerOperations.put(headerWriter.getOperationType(), headerWriter);
            headerWriter.initialize();

            /*
             ** For each Storage Server, setup a ConnectComplete operation that is used when the NIO
             **   connection is made with the StorageServer.
             */
            List<Operation> operationList = new LinkedList<>();
            operationList.add(headerBuilder);
            operationList.add(responseBufferMetering);
            ConnectComplete connectComplete = new ConnectComplete(requestContext, operationList, storageServer.getServerTcpPort());
            requestHandlerOperations.put(connectComplete.getOperationType(), connectComplete);

            /*
             ** Setup the operations to read in the HTTP Response header and process it
             */
            ReadStorageServerResponseBuffer readRespBuffer = new ReadStorageServerResponseBuffer(requestContext,
                    storageServerConnection, storageServerResponseBufferManager, respBufferPointer);
            requestHandlerOperations.put(readRespBuffer.getOperationType(), readRespBuffer);
            BufferManagerPointer httpBufferPointer = readRespBuffer.initialize();

            StorageServerResponseHandler httpRespHandler = new StorageServerResponseHandler(requestContext,
                    storageServerResponseBufferManager, httpBufferPointer, responseBufferMetering,this,
                    storageServer);
            requestHandlerOperations.put(httpRespHandler.getOperationType(), httpRespHandler);
            httpRespHandler.initialize();

            /*
             ** Now open a initiator connection to write encrypted buffers out of.
             */
            if (!storageServerConnection.startInitiator(storageServer.getServerIpAddress(),
                    storageServer.getServerTcpPort(), connectComplete, errorHandler)) {
                /*
                ** This means the SocketChannel could not be opened. Need to indicate a problem
                **   with the Storage Server and clean up this SetupChunkWrite.
                ** Set the error to indicate that the Storage Server cannot be reached.
                **
                ** TODO: Add a test case for the startInitiator failing to make sure the cleanup
                **   is properly handled.
                 */
                complete();
            }
        } else {

            complete();
        }
    }

    /*
    ** This is called to cleanup the SetupChunkWrite and will tear down all the BufferManagerPointers and
    **   release the ByteBuffer(s) associated with the storageServerBufferManager.
     */
    public void complete() {

        LOG.info("SetupChunkWrite[" + requestContext.getRequestId() + "] complete addr: " +
                storageServer.getServerIpAddress().toString() + " port: " +
                storageServer.getServerTcpPort() + " chunkNumber: " + storageServer.getChunkNumber() +
                " writer: " + writerNumber);

        /*
        ** The following must be called in order to make sure that the BufferManagerPointer
        **   dependencies are torn down in the correct order. The pointers in
        **   headerWriter are dependent upon the pointers in headerBuilder
         */
        Operation headerWriter = requestHandlerOperations.get(OperationTypeEnum.WRITE_HEADER_TO_STORAGE_SERVER);
        headerWriter.complete();
        requestHandlerOperations.remove(OperationTypeEnum.WRITE_HEADER_TO_STORAGE_SERVER);

        Operation headerBuilder = requestHandlerOperations.get(OperationTypeEnum.BUILD_HEADER_TO_STORAGE_SERVER);
        headerBuilder.complete();
        requestHandlerOperations.remove(OperationTypeEnum.BUILD_HEADER_TO_STORAGE_SERVER);

        /*
         ** The following must be called in order to make sure that the BufferManagerPointer
         **   dependencies are torn down in the correct order. The pointers in
         **   httpRespHandler are dependent upon the pointers in readRespBuffer.
         */
        Operation httpRespHandler = requestHandlerOperations.get(OperationTypeEnum.STORAGE_SERVER_RESPONSE_HANDLER);
        httpRespHandler.complete();
        requestHandlerOperations.remove(OperationTypeEnum.STORAGE_SERVER_RESPONSE_HANDLER);

        Operation readRespBuffer = requestHandlerOperations.get(OperationTypeEnum.READ_STORAGE_SERVER_RESPONSE_BUFFER);
        readRespBuffer.complete();
        requestHandlerOperations.remove(OperationTypeEnum.READ_STORAGE_SERVER_RESPONSE_BUFFER);

        /*
        ** Remove the HandleChunkWriteConnError operation from the createdOperations list. This never has its
        **   complete() method called, so it is best just to remove it.
         */
        requestHandlerOperations.remove(OperationTypeEnum.HANDLE_CHUNK_WRITE_CONN_ERROR);

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
            storageServerConnection.closeConnection();
        }
        requestContext.releaseConnection(storageServerConnection);
        storageServerConnection = null;

        /*
        ** Return the allocated buffers that were used to send the HTTP Request and the
        **   Shaw-256 value to the Storage Server
         */
        storageServerBufferManager.reset(addBufferPointer);
        for (int i = 0; i < STORAGE_SERVER_HEADER_BUFFER_COUNT; i++) {
            ByteBuffer buffer = storageServerBufferManager.getAndRemove(addBufferPointer);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer, storageServerBufferManager);
            } else {
                LOG.info("SetupChunkWrite[" + requestContext.getRequestId() + "] null buffer addBufferPointer index: " + addBufferPointer.getCurrIndex());
            }
        }

        storageServerBufferManager.unregister(addBufferPointer);
        addBufferPointer = null;

        storageServerBufferManager.reset();
        storageServerBufferManager = null;

        /*
         ** Return the allocated buffers that were used to receive the HTTP Response
         **   from the Storage Server
         */
        responseBufferMetering.complete();
        storageServerResponseBufferManager = null;

        /*
        ** Clear the HTTP Request sent for this Storage Server
         */
        requestContext.removeHttpRequestSent(storageServer);

        /*
        ** Now call back the Operation that will handle the completion
         */
        completeCallback.event();
    }

    /*
    ** This must be on a per connection basis
     */
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
        //LOG.info("SetupChunkWrite[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupChunkWrite[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupChunkWrite[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupChunkWrite[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupChunkWrite[" + requestContext.getRequestId() +
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

}
