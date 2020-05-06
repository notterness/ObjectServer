package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.buffermgr.ChunkMemoryPool;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ObjectInfo;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.WriteToClient;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ReadObjectChunks implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ReadObjectChunks.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.READ_OBJECT_CHUNKS;

    private final int MAXIMUM_NUMBER_OF_CHUNKS_PER_OBJECT = 10;

    /*
     ** The operations are all tied together via the RequestContext
     */
    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    private final ChunkMemoryPool chunkMemPool;

    /*
    ** This is to make the execute() function more manageable
     */
    enum ExecutionState {
        DETERMINE_STORAGE_SERVERS,
        SETUP_CHUNK_READ,
        WAIT_FOR_ALL_CHUNK_READS,
        SEND_RESPONSE,
        WAIT_FOR_RESPONSE_SEND,
        VERIFY_CHUNK_READ,
        ALL_CHUNKS_COMPLETED,
        EMPTY_STATE
    }

    private ReadObjectChunks.ExecutionState currState;


    /*
    ** The ObjectInfo class is used to hold the information required to read in the chunks that make up the requested
    **   object from the various Storage Servers.
     */
    private final ObjectInfo objectInfo;

    private final Operation completeCallback;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> readChunksOps;

    /*
    ** The following is the list of Storage Servers to attempt to read in chunk data from. In the event the server is
    **   not accessible, then a new server will be attempted (assuming there is a redundant server to read the data
    **   from).
     */
    private final ServerIdentifier[] serversToReadFrom;


    /*
     ** The following is used to keep track of the SetupChunkRead operation that is used to read the chunk in from the
     **   Storage Server, process the chunk and then send it to the client.
     */
    private final SetupChunkRead[] chunkReadOps;

    private int totalChunksToProcess;
    private int chunkToWrite;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;


    public ReadObjectChunks(final RequestContext requestContext, final MemoryManager memoryManager,
                            final ChunkMemoryPool chunkMemPool, final ObjectInfo objectInfo,
                            final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.chunkMemPool = chunkMemPool;
        this.objectInfo = objectInfo;
        this.completeCallback = completeCb;

        /*
         ** Setup the list of Operations currently used to handle the GET object method
         */
        readChunksOps = new HashMap<>();

        serversToReadFrom = new ServerIdentifier[MAXIMUM_NUMBER_OF_CHUNKS_PER_OBJECT];
        chunkReadOps = new SetupChunkRead[MAXIMUM_NUMBER_OF_CHUNKS_PER_OBJECT];


        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        chunkToWrite = 0;
        currState = ExecutionState.DETERMINE_STORAGE_SERVERS;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() {
        return requestContext.getRequestId();
    }

    /*
    ** THis sets up the writing of the response headers back to the client. Since there may be errors returned,
    **   the header cannot be sent until all of the object's chunks have been read up into memory.
     */
    public BufferManagerPointer initialize() {
        /*
         ** In the good path, after the HTTP Response is sent back to the client, then start reading in the chunks
         **   that make up the requested object.
         */
        SendObjectGetResponse sendResponse = new SendObjectGetResponse(requestContext, memoryManager, objectInfo);
        readChunksOps.put(sendResponse.getOperationType(), sendResponse);
        BufferManagerPointer clientWritePtr = sendResponse.initialize();

        IoInterface clientConnection = requestContext.getClientConnection();
        WriteToClient writeToClient = new WriteToClient(requestContext, clientConnection, this, clientWritePtr, null);
        readChunksOps.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        return null;
    }

    public void event() {
        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {
        switch (currState) {
            case DETERMINE_STORAGE_SERVERS:
                {
                    /*
                     ** Find one chunk for each chunkIndex and request the data for that chunk
                     */
                    int chunkIndexToFind = 0;
                    for (ServerIdentifier server : objectInfo.getChunkList()) {
                        if (server.getChunkNumber() == chunkIndexToFind) {
                            serversToReadFrom[chunkIndexToFind] = server;

                            LOG.info("ReadObjectChunks[" + requestContext.getRequestId() + "] addr: " +
                                    server.getServerIpAddress().toString() + " port: " +
                                    server.getServerTcpPort() + " chunkNumber: " + server.getChunkNumber() + " offset: " +
                                    server.getOffset() + " chunkSize: " + server.getLength());

                            chunkIndexToFind++;
                        }
                    }
                    totalChunksToProcess = chunkIndexToFind;

                    /*
                     ** Remove the servers so they will not be used again if there is an error reading from any of them
                     */
                    for (int i = 0; i < totalChunksToProcess; i++) {
                        ServerIdentifier server = serversToReadFrom[i];
                        if (server != null) {
                            objectInfo.getChunkList().remove(server);
                        }
                    }

                    LOG.info("ReadObjectChunks totalChunksToProcess: " + totalChunksToProcess);
                }

                if (totalChunksToProcess > 0) {
                    currState = ExecutionState.SETUP_CHUNK_READ;
                    /*
                     ** Fall through
                     */
                } else {
                    currState = ExecutionState.ALL_CHUNKS_COMPLETED;
                    event();
                    break;
                }

            case SETUP_CHUNK_READ:
                {
                    currState = ExecutionState.WAIT_FOR_ALL_CHUNK_READS;

                    /*
                    ** Start the reads for each chunk
                     */
                    int chunkIndex = 0;
                    while (chunkIndex < totalChunksToProcess) {
                        ServerIdentifier server = serversToReadFrom[chunkIndex];

                        if (server != null) {
                            SetupChunkRead chunkRead = new SetupChunkRead(requestContext, server, memoryManager, chunkMemPool,
                                this, server.getChunkNumber(), null);
                            chunkReadOps[chunkIndex] = chunkRead;

                            chunkRead.initialize();
                            chunkRead.event();
                        }
                        chunkIndex++;
                    }
                }
                break;

            case WAIT_FOR_ALL_CHUNK_READS:
                {
                    int chunkIndex = 0;
                    while (chunkIndex < totalChunksToProcess) {

                        ServerIdentifier server = serversToReadFrom[chunkIndex];
                        if ((server != null) && (server.getChunkNumber() == chunkToWrite)) {
                            /*
                            ** First check if this chunks write to the client has completed
                             */
                            int responseCode = server.getResponseStatus();
                            if (responseCode == HttpStatus.OK_200) {
                                /*
                                ** If the chunk read has completed, make sure that the overall status is good. If it is
                                **   not good, cleanup the ReadFromChunk operation since the data is not going to be
                                **   sent to the client.
                                 */
                                if (requestContext.getHttpParseStatus() != HttpStatus.OK_200) {
                                    /*
                                    ** Clean up the ReadFromChunk operation and remove the references
                                    */
                                    chunkReadOps[chunkToWrite].complete();
                                    chunkReadOps[chunkToWrite] = null;
                                    serversToReadFrom[chunkToWrite] = null;
                                }
                            } else if (responseCode != -1) {
                                /*
                                 ** Clean up the ReadFromChunk operation and remove the references
                                 */
                                chunkReadOps[chunkToWrite].complete();
                                chunkReadOps[chunkToWrite] = null;
                                serversToReadFrom[chunkToWrite] = null;

                                /*
                                 ** Log an error if one has not already been logged (only keep around the first failure)
                                 */
                                if (requestContext.getHttpParseStatus() == HttpStatus.OK_200) {
                                    HttpRequestInfo httpInfo = requestContext.getHttpInfo();

                                    String failureMessage = "\"Unable to obtain read chunk data - failed Storage Server\"";
                                    failureMessage += ",\n  \"StorageServer\": \"" + server.getServerName() + "\"";

                                    httpInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, failureMessage);
                                }
                            } else {
                                /*
                                ** Waiting for the chunk to be read in so there is nothing to do for now
                                */
                                break;
                            }
                        }

                        chunkIndex++;
                    }   // end of while()

                    if (chunkIndex == totalChunksToProcess) {
                        currState = ExecutionState.SEND_RESPONSE;
                        /*
                        ** Fall through
                        */
                    } else {
                        break;
                    }
                }

            case SEND_RESPONSE:
                LOG.info("SEND_RESPONSE");
                Operation sendResponse = readChunksOps.get(OperationTypeEnum.SEND_OBJECT_GET_RESPONSE);
                if (sendResponse != null) {
                    currState = ExecutionState.WAIT_FOR_RESPONSE_SEND;
                    sendResponse.event();
                } else {
                    /*
                    ** Nothing more to do, some sort of an error
                     */
                    LOG.error("SEND_RESPONSE operation is null");
                    currState = ExecutionState.ALL_CHUNKS_COMPLETED;
                    event();
                }
                break;

            case WAIT_FOR_RESPONSE_SEND:
                if (requestContext.getHttpParseStatus() != HttpStatus.OK_200) {
                    LOG.warn("WAIT_FOR_RESPONSE_SENT error");
                    currState = ExecutionState.ALL_CHUNKS_COMPLETED;
                    event();
                    break;
                }
                /*
                ** Fall through since the response was sent and it was a good status
                 */
                currState = ExecutionState.VERIFY_CHUNK_READ;

            case VERIFY_CHUNK_READ:
                {
                    int chunkIndex = 0;
                    while (chunkIndex < totalChunksToProcess) {

                        ServerIdentifier server = serversToReadFrom[chunkIndex];
                        if ((server != null) && (server.getChunkNumber() == chunkToWrite)) {
                            /*
                            ** First check if this chunks write to the client has completed
                             */
                            if (server.getClientChunkWriteDone()) {
                                LOG.info("ReadObjectChunks[" + requestContext.getRequestId() + "] writeDone addr: " +
                                        server.getServerIpAddress().toString() + " port: " +
                                        server.getServerTcpPort() + " chunkNumber: " + server.getChunkNumber());

                                /*
                                 ** Cleanup the SetupChunkRead operation and remove the references
                                 */
                                chunkReadOps[chunkToWrite].complete();
                                chunkReadOps[chunkToWrite] = null;
                                serversToReadFrom[chunkToWrite] = null;

                                chunkToWrite++;
                            } else if (server.getResponseStatus() == HttpStatus.OK_200) {
                                /*
                                 ** Since the previous chunk write to the client has complete, check if the next chunk
                                 **   is ready to be written out to the client (which is true since the status was
                                 **   OK_200).
                                 ** Only start the write to the client if there has not been an error
                                 */
                                if (requestContext.getHttpParseStatus() == HttpStatus.OK_200) {
                                    chunkReadOps[chunkToWrite].event();
                                } else {
                                    /*
                                    ** There was an error for this operation. Do not write the data back to the client.
                                     */
                                    LOG.info("ReadObjectChunks[" + requestContext.getRequestId() + "] request error addr: " +
                                            server.getServerIpAddress().toString() + " port: " +
                                            server.getServerTcpPort() + " chunkNumber: " + server.getChunkNumber());

                                    /*
                                    ** Clean up SetupChunkRead operations and remove references. Mark this chunk as
                                    **   written (back incrementing the chunkToWrite index) so the next one can be
                                    **   started.
                                     */
                                    chunkReadOps[chunkToWrite].complete();
                                    chunkReadOps[chunkToWrite] = null;
                                    serversToReadFrom[chunkToWrite] = null;

                                    chunkToWrite++;
                                }
                            } else {
                                /*
                                 ** Waiting for either the chunk to be read in or to be written to the client so there
                                 **   is nothing to do for now
                                 */
                                break;
                            }
                        }

                        chunkIndex++;
                    }   // end of while()

                    LOG.info("ReadObjectChunks VERIFY_CHUNK_READ chunkToWrite: " + chunkToWrite + " totalChunksToProcess: " +
                            totalChunksToProcess);
                    if (chunkToWrite == totalChunksToProcess) {
                        currState = ExecutionState.ALL_CHUNKS_COMPLETED;
                        /*
                        ** Fall through
                         */
                    } else {
                        break;
                    }
                }

            case ALL_CHUNKS_COMPLETED:
                LOG.info("ALL_CHUNKS_COMPLETED");
                /*
                ** For error cases, cleanup and SetupChunkRead operations that are still outstanding
                 */
                int chunkIndex = 0;
                while (chunkIndex < totalChunksToProcess) {
                    ServerIdentifier server = serversToReadFrom[chunkIndex];
                    if (server != null) {
                        chunkReadOps[chunkToWrite].complete();
                        chunkReadOps[chunkToWrite] = null;
                        serversToReadFrom[chunkToWrite] = null;
                    }
                    chunkIndex++;
                }

                completeCallback.event();
                currState = ExecutionState.EMPTY_STATE;
                break;

            case EMPTY_STATE:
                break;
        }
    }

    public void complete() {
        LOG.info("ReadObjectChunks[" + requestContext.getRequestId() + "] complete()");

        Operation writeToClient = readChunksOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        writeToClient.complete();

        Operation sendResponse = readChunksOps.remove(OperationTypeEnum.SEND_OBJECT_GET_RESPONSE);
        sendResponse.complete();

        /*
         ** Call the complete() method for any operations that this one created.
         */
        Collection<Operation> createdOperations = readChunksOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        readChunksOps.clear();
    }

    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when an Operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ReadObjectChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ReadObjectChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ReadObjectChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ReadObjectChunks[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ReadObjectChunks[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = readChunksOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
