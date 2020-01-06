package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.athena.webserver.requestcontext.ServerIdentifier;
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
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_CHUNK_WRITE;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
    ** The StorageServerIdentifer is this chunk write's unique identifier. It is determined through the VON
    **   checker. It identifies the chunk and the Storage Server through the  IP address, Port number and
    **   the chunk number.
     */
    private final ServerIdentifier serverIdentifier;

    private final MemoryManager memoryManager;

    private final Operation completeCallback;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    private final BufferManagerPointer encryptedBufferPtr;

    private final int chunkBytesToEncrypt;

    private BufferManager storageServerBufferManager;
    private BufferManager storageServerResponseBufferManager;

    private BufferManagerPointer addBufferPointer;
    private BufferManagerPointer respBufferPointer;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> requestHandlerOperations;

    /*
    ** The following is the connection used to communicate with the Storage Server
     */
    private IoInterface storageServerConnection;

    private boolean chunkWriteSetupComplete;

    /*
    ** SetupChunkWrite is called at the beginning of each chunk (128MB) block of data. This is what sets
    **   up the calls to obtain the VON information and the meta-data write to the database.
     */
    public SetupChunkWrite(final RequestContext requestContext, final ServerIdentifier serverIdentifier,
                           final MemoryManager memoryManager, final BufferManagerPointer encryptedBufferPtr,
                           final int chunkBytesToEncrypt, final Operation completeCb) {

        this.requestContext = requestContext;
        this.serverIdentifier = serverIdentifier;
        this.memoryManager = memoryManager;
        this.encryptedBufferPtr = encryptedBufferPtr;
        this.chunkBytesToEncrypt = chunkBytesToEncrypt;

        this.completeCallback = completeCb;

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        /*
         ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        requestHandlerOperations = new HashMap<>();

        chunkWriteSetupComplete = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

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
            chunkWriteSetupComplete = true;

            /*
             ** Create a BufferManager with two required entries to send the HTTP Request header to the
             **   Storage Server and then to send the final Shaw-256 calculation.
             */
            storageServerBufferManager = new BufferManager(STORAGE_SERVER_HEADER_BUFFER_COUNT,
                    "StorageServer", 1000);

            /*
             ** Create a BufferManager to accept the HTTP Response from the Storage Server
             */
            storageServerResponseBufferManager = new BufferManager(STORAGE_SERVER_HEADER_BUFFER_COUNT,
                    "StorageServerResponse", 1100);

            /*
             ** Allocate ByteBuffer(s) for the header and the Shaw-256
             */
            addBufferPointer = storageServerBufferManager.register(this);
            storageServerBufferManager.bookmark(addBufferPointer);
            for (int i = 0; i < STORAGE_SERVER_HEADER_BUFFER_COUNT; i++) {
                ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, storageServerBufferManager);

                storageServerBufferManager.offer(addBufferPointer, buffer);
            }

            storageServerBufferManager.reset(addBufferPointer);

            /*
             ** Allocate ByteBuffer(s) to read in the HTTP Response from the Storage Server
             */
            respBufferPointer = storageServerResponseBufferManager.register(this);
            storageServerResponseBufferManager.bookmark(respBufferPointer);
            for (int i = 0; i < STORAGE_SERVER_HEADER_BUFFER_COUNT; i++) {
                ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, storageServerResponseBufferManager);

                storageServerResponseBufferManager.offer(respBufferPointer, buffer);
            }

            storageServerResponseBufferManager.reset(respBufferPointer);

            /*
             ** For each Storage Server, setup a HandleStorageServerError operation that is used when there
             **   is an error communicating with the StorageServer.
             */
            HandleStorageServerError errorHandler = new HandleStorageServerError(requestContext);
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
                    encryptedBufferPtr, chunkBytesToEncrypt, serverIdentifier);
            requestHandlerOperations.put(storageServerWriter.getOperationType(), storageServerWriter);
            storageServerWriter.initialize();

            /*
             ** The PUT Header must be written to the Storage Server prior to sending the data
             */
            BuildHeaderToStorageServer headerBuilder = new BuildHeaderToStorageServer(requestContext, storageServerConnection,
                    storageServerBufferManager, addBufferPointer, chunkBytesToEncrypt);
            requestHandlerOperations.put(headerBuilder.getOperationType(), headerBuilder);
            BufferManagerPointer writePointer = headerBuilder.initialize();

            List<Operation> ops = new LinkedList<>();
            ops.add(storageServerWriter);
            WriteHeaderToStorageServer headerWriter = new WriteHeaderToStorageServer(requestContext, storageServerConnection, ops,
                    storageServerBufferManager, writePointer, serverIdentifier);
            requestHandlerOperations.put(headerWriter.getOperationType(), headerWriter);
            headerWriter.initialize();

            /*
             ** For each Storage Server, setup a ConnectComplete operation that is used when the NIO
             **   connection is made with the StorageServer.
             */
            List<Operation> operationList = new LinkedList<>();
            operationList.add(headerBuilder);
            ConnectComplete connectComplete = new ConnectComplete(requestContext, operationList,
                    RequestContext.STORAGE_SERVER_PORT_BASE);
            requestHandlerOperations.put(connectComplete.getOperationType(), connectComplete);

            /*
             ** Setup the operations to read in the HTTP Response header and process it
             */
            ReadStorageServerResponseBuffer readRespBuffer = new ReadStorageServerResponseBuffer(requestContext,
                    storageServerConnection, storageServerResponseBufferManager, respBufferPointer);
            requestHandlerOperations.put(readRespBuffer.getOperationType(), readRespBuffer);
            BufferManagerPointer httpBufferPointer = readRespBuffer.initialize();

            StorageServerResponseHandler httpRespHandler = new StorageServerResponseHandler(requestContext,
                    storageServerResponseBufferManager, httpBufferPointer, this,
                    serverIdentifier);
            requestHandlerOperations.put(httpRespHandler.getOperationType(), httpRespHandler);
            httpRespHandler.initialize();

            /*
             ** Meter out a buffer to allow the Storage Server HTTP response to be read in
             */
            storageServerResponseBufferManager.updateProducerWritePointer(respBufferPointer);

            /*
             ** Now open a initiator connection to write encrypted buffers out of.
             */
            storageServerConnection.startInitiator(serverIdentifier.getStorageServerIpAddress(),
                    serverIdentifier.getStorageServerTcpPort(), connectComplete, errorHandler);
        } else {

            complete();
        }
    }

    /*
     */
    public void complete() {

        LOG.info("SetupChunkWrite[" + requestContext.getRequestId() + "] complete");

        /*
        ** Close out the connection used to communicate with the Storage Server. Then
        ** clear out the reference to the connection so it may be released back to the pool.
         */
        storageServerConnection.closeConnection();
        requestContext.releaseConnection(storageServerConnection);
        storageServerConnection = null;

        /*
        ** The following must be called in order to make sure that the BufferManagerPointer
        **   dependencies are torn down in the correct order. The pointers in
        **   headerWriter are dependent upon the pointers in headerBuilder
         */
        Operation headerWriter = requestHandlerOperations.get(OperationTypeEnum.WRITE_HEADER_TO_STORAGE_SERVER);
        headerWriter.complete();
        requestHandlerOperations.remove(OperationTypeEnum.WRITE_HEADER_TO_STORAGE_SERVER);

        Operation headerBuilder = requestHandlerOperations.get(OperationTypeEnum.BUILD_HEADER_TO_STORGE_SERVER);
        headerBuilder.complete();
        requestHandlerOperations.remove(OperationTypeEnum.BUILD_HEADER_TO_STORGE_SERVER);

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
        ** Call the complete() methods for all of the Operations created to handle the chunk write that did not have
        **   ordering dependencies due to the registrations with the BufferManager(s).
         */
        Collection<Operation> createdOperations = requestHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        requestHandlerOperations.clear();

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
        storageServerBufferManager.reset();
        storageServerBufferManager = null;

        /*
         ** Return the allocated buffers that were used to receive the HTTP Response
         **   from the Storage Server
         */
        storageServerResponseBufferManager.reset(respBufferPointer);
        for (int i = 0; i < STORAGE_SERVER_HEADER_BUFFER_COUNT; i++) {
            ByteBuffer buffer = storageServerResponseBufferManager.getAndRemove(respBufferPointer);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer, storageServerResponseBufferManager);
            } else {
                LOG.info("SetupChunkWrite[" + requestContext.getRequestId() + "] null buffer respBufferPointer index: " +
                        respBufferPointer.getCurrIndex());
            }
        }

        storageServerResponseBufferManager.unregister(respBufferPointer);
        storageServerResponseBufferManager.reset();
        storageServerResponseBufferManager = null;

        /*
        ** Clear the HTTP Request sent for this Storage Server
         */
        requestContext.removeHttpRequestSent(serverIdentifier);

        /*
        ** Now call back the Operation that will handle the completion
         */
        completeCallback.event();
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
     ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
     **   of which queue the connection is on. It will probably clean up the code some.
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (onDelayedQueue) {
            if (!delayedExecutionQueue) {
                LOG.warn("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
            }

            onDelayedQueue = false;
            nextExecuteTime = 0;
        } else if (onExecutionQueue){
            if (delayedExecutionQueue) {
                LOG.warn("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on workQueue");
            }

            onExecutionQueue = false;
        } else {
            LOG.warn("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            nextExecuteTime = System.currentTimeMillis() + TIME_TILL_NEXT_TIMEOUT_CHECK;
            onDelayedQueue = true;
        } else {
            onExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return onDelayedQueue;
    }

    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        //LOG.info("requestId[" + requestContext.getRequestId() + "] waitTimeElapsed " + currTime);
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
