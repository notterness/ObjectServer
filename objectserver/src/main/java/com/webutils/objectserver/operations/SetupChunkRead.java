package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.buffermgr.ChunkAllocBufferInfo;
import com.webutils.webserver.buffermgr.ChunkMemoryPool;
import com.webutils.webserver.common.ResponseMd5ResultHandler;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.*;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class SetupChunkRead implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupChunkRead.class);

    private final int STORAGE_SERVER_HEADER_BUFFER_COUNT = 4;
    private final int STORAGE_SERVER_GET_BUFFER_COUNT = 10;

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

    private final ChunkMemoryPool chunkMemPool;

    private final Operation completeCallback;

    /*
     ** This is to make the execute() function more manageable
     */
    enum ExecutionState {
        SETUP_CHUNK_READ_OPS,
        WAITING_FOR_CONN_COMP,
        WAITING_FOR_RESPONSE_HEADER,
        READ_CONTENT_DATA,
        CONTENT_PROCESSED,
        EMPTY_STATE
    }

    private SetupChunkRead.ExecutionState currState;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** An integer that identifies which of the Storage Server writers this is
     */
    private final int writerNumber;

    /*
     ** The following is set to null in normal cases or it is set to a value when the ChunkWrite want the target Storage
     **   Server to respond with an error or to close the TCP connection at certain times during the transfer.
     */
    private final String errorInjectString;

    private BufferManager requestBufferManager;
    private BufferManager responseBufferManager;

    /*
    ** The decryptedBufferManager comes from a pool of pre-allocated BufferManagers that have a full chunks worth of
    **   ByteBuffers allocated to them.
     */
    private BufferManager decryptBufferManager;
    private BufferManagerPointer decryptMeteringPointer;
    private Operation decryptBufferMetering;

    private BufferManagerPointer addBufferPointer;

    /*
    ** The response buffer metering is used by the DecryptBuffer operation
     */
    private StorageServerResponseBufferMetering responseBufferMetering;

    private BufferManagerPointer httpBufferPointer;

    /*
    ** The DecryptBuffer operation needs to be check for completion
     */
    private DecryptBuffer decryptBuffer;

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
    private IoInterface storageServerConnection;

    private boolean serverConnectionClosedDueToError;

    /*
    ** The updater used to manage the Md5 digest and its result
     */
    private ResponseMd5ResultHandler updater;

    /*
     ** SetupChunkWrite is called at the beginning of each chunk (128MB) block of data. This is what sets
     **   up the calls to obtain the VON information and the meta-data write to the database.
     */
    public SetupChunkRead(final RequestContext requestContext, final ServerIdentifier server,
                          final MemoryManager memoryManager, final ChunkMemoryPool chunkMemPool,
                          final Operation completeCb, final int writer, final String errorInjectString) {

        this.operationType = OperationTypeEnum.fromInt(OperationTypeEnum.SETUP_CHUNK_READ_0.toInt() + writer);
        this.requestContext = requestContext;
        this.storageServer = server;
        this.memoryManager = memoryManager;
        this.chunkMemPool = chunkMemPool;

        this.completeCallback = completeCb;

        this.writerNumber = writer;

        this.errorInjectString = errorInjectString;

        /*
         ** First setup the HttpResponseInfo for this request.
         */
        this.httpInfo = new HttpResponseInfo(requestContext);
        this.storageServer.setHttpInfo(httpInfo);

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        requestHandlerOperations = new HashMap<>();

        currState = ExecutionState.SETUP_CHUNK_READ_OPS;

        serverConnectionClosedDueToError = false;

        LOG.info("SetupChunkRead[" + requestContext.getRequestId() + "] addr: " +
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

        /*
         ** Allocate a BufferManager to hold the decrypted data. THis one must be the size of a chunk since it will
         **   not be drained until the chunk read is complete and the Md5 digest has been validated.
         ** The use of a different BufferManager to hold the data prior to pushing it to the client allows for
         **   failures by a Storage Server to be handled easily. It would be much harder to handle a failure if
         **   part of the data had been pushed to the client and then a Storage Server stopped sending data.
         */
        ChunkAllocBufferInfo allocInfo = chunkMemPool.allocateChunk(requestContext);
        if (allocInfo != null) {
            decryptBufferManager = allocInfo.getBufferManager();
            decryptMeteringPointer = allocInfo.getAddBufferPointer();
            decryptBufferMetering = allocInfo.getMetering();
        }

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
            case SETUP_CHUNK_READ_OPS:
                if (setupChunkReadOps()) {
                    currState = ExecutionState.WAITING_FOR_CONN_COMP;
                } else {
                    currState = ExecutionState.EMPTY_STATE;
                }
                break;

            case WAITING_FOR_CONN_COMP:
                {
                    /*
                     ** First check if all the request has been sent to the StorageServer (meaning the connection
                     **   was made and the data has been written)
                     */
                    int status;
                    if (requestContext.hasHttpRequestBeenSent(storageServer)) {
                        currState = ExecutionState.WAITING_FOR_RESPONSE_HEADER;
                        /*
                        ** Fall through to handle the case where the connection was setup and the response header was
                        **   received prior to this being scheduled.
                         */
                    } else if ((status = requestContext.getStorageResponseResult(storageServer)) != HttpStatus.OK_200) {
                        LOG.warn("SetupChunkRead[" + requestContext.getRequestId() + "] failure: " + status + " addr: " +
                            storageServer.getServerIpAddress().toString() + " port: " +
                            storageServer.getServerTcpPort() + " chunkNumber: " + storageServer.getChunkNumber() +
                            " writer: " + writerNumber);

                        /*
                        ** TODO: This needs to check if there is another storage server
                         */
                        StringBuilder failureMessage = new StringBuilder("\"Unable to obtain read chunk data - failed Storage Server\"");
                        failureMessage.append(",\n  \"StorageServer\": \"").append(storageServer.getServerName()).append("\"");

                        requestContext.getHttpInfo().setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, failureMessage.toString());

                        currState = ExecutionState.EMPTY_STATE;
                        complete();
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
                        event();
                    } else if (status != -1) {
                        /*
                        ** Some sort of an error response
                         */
                        currState = ExecutionState.EMPTY_STATE;
                        complete();
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
                boolean allDecrypted = decryptBuffer.getBuffersAllDecrypted();
                LOG.info("CONTENT_PROCESSED md5Complete: " + md5Complete + " allDecrypted: " + allDecrypted);

                if (md5Complete && allDecrypted) {
                    if (updater.checkContentMD5(storageServer.getMd5Digest())) {

                    }
                    LOG.info("chunk read complete");

                    complete();
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

        LOG.info("SetupChunkRead[" + requestContext.getRequestId() + "] complete addr: " +
                storageServer.getServerIpAddress().toString() + " port: " +
                storageServer.getServerTcpPort() + " chunkNumber: " + storageServer.getChunkNumber() +
                " writer: " + writerNumber);

        /*
         ** The following must be called in order to make sure that the BufferManagerPointer
         **   dependencies are torn down in the correct order. The pointers in
         **   headerWriter are dependent upon the pointers in headerBuilder
         */
        Operation headerWriter = requestHandlerOperations.remove(OperationTypeEnum.WRITE_HEADER_TO_STORAGE_SERVER);
        headerWriter.complete();

        Operation headerBuilder = requestHandlerOperations.remove(OperationTypeEnum.BUILD_HEADER_TO_STORAGE_SERVER);
        headerBuilder.complete();

        Operation processResponse = requestHandlerOperations.remove(OperationTypeEnum.STORAGE_SERVER_RESPONSE_HANDLER);
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

        Operation decryptBuffer = requestHandlerOperations.remove(OperationTypeEnum.DECRYPT_BUFFER);
        if (decryptBuffer != null) {
            decryptBuffer.complete();
        }

        Operation readBuffer = requestHandlerOperations.remove(OperationTypeEnum.READ_BUFFER);
        readBuffer.complete();

        /*
         ** Remove the HandleChunkReadConnError operation from the createdOperations list. This never has its
         **   complete() method called, so it is best just to remove it.
         */
        requestHandlerOperations.remove(OperationTypeEnum.HANDLE_CHUNK_READ_CONN_ERROR);

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
         ** Return the allocated buffers that were used to send the GET Request to the Storage Server
         */
        requestBufferManager.reset(addBufferPointer);
        for (int i = 0; i < STORAGE_SERVER_HEADER_BUFFER_COUNT; i++) {
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
        requestContext.removeHttpRequestSent(storageServer);

        /*
        ** remove the HttpResponseInfo association from the ServerIdentifier
         */
        storageServer.setHttpInfo(null);

        /*
         ** Now call back the Operation that will handle the completion
         */
        completeCallback.event();
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
        //LOG.info("SetupChunkRead[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupChunkRead[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupChunkRead[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupChunkRead[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupChunkRead[" + requestContext.getRequestId() +
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
    ** This sets up the operations and all of their dependencies.
    **
    ** It will return false if the setup of the connection to the Storage Server fails
     */
    private boolean setupChunkReadOps() {
        /*
         ** Create a BufferManager with two required entries to send the HTTP Request header to the
         **   Storage Server.
         */
        requestBufferManager = new BufferManager(STORAGE_SERVER_HEADER_BUFFER_COUNT,
                "StorageServer", 1000 + (writerNumber * 10));

        /*
         ** Allocate ByteBuffer(s) for the GET request header
         */
        addBufferPointer = requestBufferManager.register(this);
        requestBufferManager.bookmark(addBufferPointer);
        for (int i = 0; i < STORAGE_SERVER_HEADER_BUFFER_COUNT; i++) {
            ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, requestBufferManager,
                    operationType);

            requestBufferManager.offer(addBufferPointer, buffer);
        }

        requestBufferManager.reset(addBufferPointer);

        /*
         ** Create a BufferManager to accept the response from the Storage Server. This BufferManager is just used
         **   as a placeholder for buffers while there are decrypted and have their Md5 digest computed. After they
         **   are decrypted, they are placed in the chunk buffer to be written to the client.
         **
         */
        responseBufferManager = new BufferManager(STORAGE_SERVER_GET_BUFFER_COUNT,
                "StorageServerResponse", 1100 + (writerNumber * 10));

        /*
         ** Allocate ByteBuffer(s) to read in the response from the Storage Server. By using a metering operation, the
         **   setup for the reading of the Storage Server response header can be be deferred until the TCP connection to the
         **   Storage Server is successful.
         */
        responseBufferMetering = new StorageServerResponseBufferMetering(requestContext, memoryManager, responseBufferManager,
                STORAGE_SERVER_GET_BUFFER_COUNT);
        BufferManagerPointer respBufferPointer = responseBufferMetering.initialize();

        /*
         ** For each Storage Server, setup a HandleChunkWriteConnError operation that is used when there
         **   is an error communicating with the StorageServer.
         */
        HandleChunkReadConnError errorHandler = new HandleChunkReadConnError(requestContext, this, storageServer);
        requestHandlerOperations.put(errorHandler.getOperationType(), errorHandler);

        /*
         ** For each Storage Server, create the connection used to communicate with it.
         */
        storageServerConnection = requestContext.allocateConnection(this);

        /*
         ** The GET Header must be written to the Storage Server so that the data can be read in
         */
        BuildHeaderToStorageServer headerBuilder = new BuildHeaderToStorageServer(requestContext, requestBufferManager,
                addBufferPointer, storageServer, errorInjectString);
        requestHandlerOperations.put(headerBuilder.getOperationType(), headerBuilder);
        BufferManagerPointer writePointer = headerBuilder.initialize();

        List<Operation> ops = new LinkedList<>();
        ops.add(this);
        WriteHeaderToStorageServer headerWriter = new WriteHeaderToStorageServer(requestContext, storageServerConnection, ops,
                requestBufferManager, writePointer, storageServer);
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
        ReadBuffer readBuffer = new ReadBuffer(requestContext, responseBufferManager, respBufferPointer, storageServerConnection);
        requestHandlerOperations.put(readBuffer.getOperationType(), readBuffer);
        httpBufferPointer = readBuffer.initialize();


        StorageServerResponseHandler httpRespHandler = new StorageServerResponseHandler(requestContext,
                responseBufferManager, httpBufferPointer, responseBufferMetering,this,
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
        Operation httpRespHandler = requestHandlerOperations.get(OperationTypeEnum.STORAGE_SERVER_RESPONSE_HANDLER);
        httpRespHandler.complete();
        requestHandlerOperations.remove(OperationTypeEnum.STORAGE_SERVER_RESPONSE_HANDLER);

        /*
         ** The next Operations are run once the response header has been received. The routines are to
         **   compute the Md5 digest and decrypt all the data into the chunk buffer.
         **
         ** NOTE: The initialize() methods are not called until after the header has been processed.
         */
        List<Operation> opsToRun = new LinkedList<>();
        opsToRun.add(this);

        updater = new ResponseMd5ResultHandler(requestContext);
        ComputeMd5Digest md5Digest = new ComputeMd5Digest(requestContext, opsToRun, httpBufferPointer,
                responseBufferManager, updater, httpInfo.getContentLength());
        requestHandlerOperations.put(md5Digest.getOperationType(), md5Digest);
        md5Digest.initialize();

        decryptBuffer = new DecryptBuffer(requestContext, responseBufferMetering, responseBufferManager,
                httpBufferPointer, decryptBufferMetering, decryptBufferManager, decryptMeteringPointer,
                httpInfo.getContentLength(),this);
        requestHandlerOperations.put(decryptBuffer.getOperationType(), decryptBuffer);
        decryptBuffer.initialize();
    }
}
