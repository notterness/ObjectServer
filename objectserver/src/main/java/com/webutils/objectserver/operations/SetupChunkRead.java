package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.buffermgr.ChunkAllocBufferInfo;
import com.webutils.webserver.buffermgr.ChunkMemoryPool;
import com.webutils.webserver.common.ResponseMd5ResultHandler;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.StorageChunkTableMgr;
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

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType;


    private final int STORAGE_SERVER_HEADER_BUFFER_COUNT = 4;
    private final int STORAGE_SERVER_GET_BUFFER_COUNT = 10;

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */

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
        WRITE_CHUNK_TO_CLIENT,
        CALLBACK_OPS_CHUNK_READ,
        CHUNK_DATA_WRITTEN,
        CALLBACK_OPS,
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
    private ChunkAllocBufferInfo allocInfo;
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
    private IoInterface storageServerConn;

    private boolean serverConnectionClosedDueToError;

    private WriteChunkToClient writeChunkToClient;

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
                          final Operation completeCb, final String errorInjectString) {


        this.requestContext = requestContext;
        this.storageServer = server;
        this.memoryManager = memoryManager;
        this.chunkMemPool = chunkMemPool;

        this.completeCallback = completeCb;

        this.writerNumber = server.getChunkNumber();
        this.operationType = OperationTypeEnum.fromInt(OperationTypeEnum.SETUP_CHUNK_READ_0.toInt() + this.writerNumber);

        this.errorInjectString = errorInjectString;

        /*
         ** First setup the HttpResponseInfo for this request.
         */
        this.httpInfo = new HttpResponseInfo(requestContext.getRequestId());
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
                storageServer.getChunkLBA() + " writer: " + this.writerNumber);
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
        allocInfo = chunkMemPool.allocateChunk(requestContext);
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
     ** The execute method is actually a state machine that goes through the following states:
     **
     **   SETUP_CHUNK_READ - This is where the various operations that are required to read in a chunk from a Storage
     **     Server are set up. The final step is to try and create the connection to the Storage Server. If this
     **     is successful, the next step is to wait for the connection to be completed.
     **   WAITING_FOR_CONN_COMP - This is called back when the connection is complete and the GET chunk request has
     **     been sent to the Storage Server.
     **   WAITING_FOR_RESPONSE_HEADER - This is the state this operation waits at until the response has been
     **     sent back by the Storage Server. If the response is good, then the next step is to read in the data
     **     into the chunk buffer. This is done in the READ_CONTENT_DATA state.
     **   READ_CONTENT_DATA - This cleans up the StorageServerResponseHandler operation (which is used to parse the
     **     response headers) as it is no longer needed, It sets up the dependencies for the DecryptBuffer and
     **     Md5Digest operations on the ReadBuffer operation. It also sets up the WriteChunkToClient operation
     **     with a dependency upon the DecryptBuffer operation. At this point, the data is brought into the
     **     Object Server and an Md5 digest is computed along with the data being decrypted.
     **   CONTENT_PROCESSED - This waits for all the data to be decrypted and the Md5 digest to be computed. This then
     **     calls back the ReadObjectChunks operation (the overall manager for reading in the various chunks of data
     **     that make up the object). This is required since all of the chunks of data need to be read in prior to
     **     sending the response back to the client (to handle the case where there are errors and the object's
     **     data cannot be returned). Once all of the chunks are read into the Object Server and the response has been
     **     sent to the client, then the SetupChunkRead is woken up again to send the data to the client.
     **   WRITE_CHUNK_TO_CLIENT - This starts up the WriteChunkToClient operation to stream the data to the client.
     **   CHUNK_DATA_WRITTEN - This is the state when all of the chunk data has been written to the client. This wakes
     **     up the ReadObjectChunks operation to let it know that the next chunk of data can be sent to the client.
     **     This is done so that the chunks are sent to the client in the correct order to actually recreate the
     **     object.
     **   CALLBACK_OPS - This checks if there were any errors reading the data and updates the chunks record in the
     **     database. It then wakes up the ReadObjectChunks to let it know that it is complete.
     **
     */
    public void execute() {
        switch (currState) {
            case SETUP_CHUNK_READ_OPS:
                if (setupChunkReadOps()) {
                    currState = ExecutionState.WAITING_FOR_CONN_COMP;
                } else {
                    /*
                    ** This is the case where the connection to the Storage Server could not be established
                     */
                    LOG.warn("SetupChunkRead[" + requestContext.getRequestId() + "] setupInitiator failure addr: " +
                            storageServer.getServerIpAddress().toString() + " port: " +
                            storageServer.getServerTcpPort() + " chunkNumber: " + storageServer.getChunkNumber());

                    storageServer.setResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                    currState = ExecutionState.CALLBACK_OPS;
                    event();
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
                            storageServer.getServerTcpPort() + " chunkNumber: " + storageServer.getChunkNumber());

                        storageServer.setResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);

                        currState = ExecutionState.CALLBACK_OPS;
                        event();
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
                        storageServer.setResponseStatus(HttpStatus.OK_200);
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
                boolean allDecrypted = decryptBuffer.getBuffersAllDecrypted();
                LOG.info("CONTENT_PROCESSED md5Complete: " + md5Complete + " allDecrypted: " + allDecrypted);

                if (md5Complete && allDecrypted) {
                    if (updater.checkContentMD5(storageServer.getMd5Digest())) {
                        storageServer.setResponseStatus(HttpStatus.OK_200);
                    } else {
                        /*
                        ** This is a special case that requires the chunk to be marked offline since it has a bad
                        **   Md5 digest.
                         */
                        storageServer.setResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY_422);
                    }
                    /*
                     ** Fall through
                     */
                    currState = ExecutionState.CALLBACK_OPS_CHUNK_READ;
                } else {
                    /*
                    ** Still waiting for the md5 and decrypt operations to complete
                     */
                    break;
                }

            case CALLBACK_OPS_CHUNK_READ:
                /*
                ** This wakes up the ReadObjectChunk operation to determine what needs to be done with this chunks
                **   worth of data. The options are:
                **     - Assuming the chunk was read in successfully and it passed the Md5 digest check, it will
                **         be queued up to be written up to the client in the correct order. That is contingent upon
                **         the client still being able to accept data and the prior chunk being successfully written
                **         back to the client.
                **     - If there was an error with this chunk being read from the Storage Server, then a new attempt
                **         to read the chunk will be made from a different Storage Server (assuming one is available).
                **
                 */
                if (storageServer.getResponseStatus() == HttpStatus.OK_200) {
                    currState = ExecutionState.WRITE_CHUNK_TO_CLIENT;

                    /*
                     ** Now call back the ReadObjectChunks Operation that will handle the collection of chunks
                     */
                    completeCallback.event();
                } else {
                    /*
                    ** There was an error reading in this chunk so mark a chunk read error and then exit this operation.
                    ** The marking of the read error actually takes place in the CALLBACK_OPS state to keep it in
                    **   one place.
                     */
                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                }
                break;

            case WRITE_CHUNK_TO_CLIENT:
                LOG.info("SetupChunkRead WRITE_CHUNK_TO_CLIENT");
                currState = ExecutionState.CHUNK_DATA_WRITTEN;
                writeChunkToClient.initialize();
                writeChunkToClient.event();
                break;

            case CHUNK_DATA_WRITTEN:
                LOG.info("SetupChunkRead CHUNK_DATA_WRITTEN dataWritten: " + storageServer.getClientChunkWriteDone());

                /*
                **
                 */
                if (storageServer.getClientChunkWriteDone()) {
                    currState = ExecutionState.CALLBACK_OPS;
                    /*
                    ** This chunk has been written out to the client. Finish up the operation and call back the
                    **   ReadObjectChunks operation to let it proceed to the next chunk (if there is one).
                    **
                    ** Fall through to the CALLBACK_OPS state.
                     */
                 } else {
                    break;
                }

            case CALLBACK_OPS:
                int status = storageServer.getResponseStatus();
                LOG.info("CALLBACK_OPS status: " + status);

                if (status != HttpStatus.OK_200) {
                    StorageChunkTableMgr chunkTableMgr = new StorageChunkTableMgr(requestContext.getWebServerFlavor(),
                            requestContext.getHttpInfo());

                    /*
                    ** Special case when the Md5 digest from the chunk read up doesn't match what was expected, mark
                    **   that chunk offline.
                     */
                    if (status == HttpStatus.UNPROCESSABLE_ENTITY_422) {
                        chunkTableMgr.incrementChunkReadFailure(storageServer.getChunkId());
                    } else {
                        /*
                         ** There was an error reading in this chunk so mark a chunk read error and then exit this operation
                         */
                        chunkTableMgr.incrementChunkReadFailure(storageServer.getChunkId());
                    }
                }

                currState = ExecutionState.EMPTY_STATE;

                /*
                 ** Now call back the ReadObjectChunks Operation that will handle the collection of chunks
                 */
                completeCallback.event();
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

        LOG.info("SetupChunkRead[" + requestContext.getRequestId() + "] complete() addr: " +
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

        Operation writeChunk = requestHandlerOperations.remove(OperationTypeEnum.WRITE_CHUNK_TO_CLIENT);
        if (writeChunk != null) {
            writeChunk.complete();
            writeChunkToClient = null;
        }

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
            storageServerConn.closeConnection();
        }
        requestContext.releaseConnection(storageServerConn);
        storageServerConn = null;

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
         ** Release the Chunk Buffer
         */
        chunkMemPool.releaseChunk(allocInfo);
    }

    public void connectionCloseDueToError() {
        serverConnectionClosedDueToError = true;
    }

    public void setAllDataWritten() { storageServer.setClientChunkWriteDone(); }

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
        storageServerConn = requestContext.allocateConnection(this);

        /*
         ** The GET Header must be written to the Storage Server so that the data can be read in
         */
        BuildHeaderToStorageServer headerBuilder = new BuildHeaderToStorageServer(requestContext, requestBufferManager,
                addBufferPointer, storageServer, errorInjectString);
        requestHandlerOperations.put(headerBuilder.getOperationType(), headerBuilder);
        BufferManagerPointer writePointer = headerBuilder.initialize();

        List<Operation> ops = new LinkedList<>();
        ops.add(this);
        WriteHeaderToStorageServer headerWriter = new WriteHeaderToStorageServer(requestContext, storageServerConn, ops,
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
        ReadBuffer readBuffer = new ReadBuffer(requestContext, responseBufferManager, respBufferPointer, storageServerConn);
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
        if (!storageServerConn.startInitiator(storageServer.getServerIpAddress(),
                storageServer.getServerTcpPort(), connectComplete, errorHandler)) {
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
        BufferManagerPointer decryptedPtr = decryptBuffer.initialize();

        IoInterface clientConnection = requestContext.getClientConnection();
        writeChunkToClient = new WriteChunkToClient(requestContext, clientConnection, this,
                decryptBufferManager, decryptedPtr);
        requestHandlerOperations.put(writeChunkToClient.getOperationType(), writeChunkToClient);
    }
}
