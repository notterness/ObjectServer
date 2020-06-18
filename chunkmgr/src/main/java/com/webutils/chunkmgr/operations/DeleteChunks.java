package com.webutils.chunkmgr.operations;

import com.webutils.chunkmgr.http.DeleteChunksContent;
import com.webutils.webserver.common.ChunkDeleteInfo;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.chunkmgr.common.StorageServerDeleteChunkParams;
import com.webutils.webserver.http.*;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerChunkMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.SendRequestToService;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class DeleteChunks implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteChunks.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.DELETE_CHUNKS;

    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    private final DeleteChunksContent deleteChunksContent;

    private final Operation completeCallback;

    /*
     ** The following are the states the DeleteChunk needs to go through to change the state of the chunks
     **   and remove the chunks from the Storage Servers
     */
    private enum ExecutionState {
        SET_CHUNKS_TO_DELETED,
        DELETE_CHUNKS_FROM_STORAGE_SERVERS,
        SET_CHUNKS_TO_AVAILABLE,
        CALLBACK_OPS,
        EMPTY_STATE
    }

    private ExecutionState currState;

    /*
    ** The following are the accessor classes to get to the StorageServerChunk and ServerIdentifier tables
    **   in the ServiceServersDb database.
     */
    private final ServerChunkMgr chunkMgr;

    private final List<ChunkDeleteInfo> chunks;
    private final List<ServerIdentifier> servers;

    private final List<SendRequestToService> serviceRequests;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public DeleteChunks(final RequestContext requestContext, final MemoryManager memoryManager,
                        final DeleteChunksContent deleteChunksContent, final Operation completeCb) {
        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.deleteChunksContent = deleteChunksContent;
        this.completeCallback = completeCb;

        currState = ExecutionState.SET_CHUNKS_TO_DELETED;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        this.chunkMgr = new ServerChunkMgr(requestContext.getWebServerFlavor());

        this.chunks = new LinkedList<>();
        this.servers = new LinkedList<>();
        this.serviceRequests = new LinkedList<>();
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

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
     */
    public void execute() {
        switch (currState) {
            case SET_CHUNKS_TO_DELETED:
                deleteChunksContent.extractDeletions(chunks);

                LOG.info("chunks size() " + chunks.size());
                if (chunks.size() != 0) {
                    for (ChunkDeleteInfo chunk: chunks) {
                        chunkMgr.updateChunkStatus(chunk.getChunkUID(), ChunkStatusEnum.CHUNK_DELETED);
                    }

                    requestContext.getHttpInfo().setResponseHeaders(buildSuccessHeader());
                } else {
                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_REQUIRED_428 +
                            "\r\n  \"message\": \"No chunks to delete\"" +
                            "\r\n}";
                    requestContext.getHttpInfo().emitMetric(HttpStatus.PRECONDITION_REQUIRED_428, failureMessage);
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.PRECONDITION_REQUIRED_428);

                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                    break;
                }

                currState = ExecutionState.DELETE_CHUNKS_FROM_STORAGE_SERVERS;
                /*
                ** Uncomment out the following lines to skip sending the DeleteChunk request to the Storager Servder
                currState = ExecutionState.CALLBACK_OPS;
                event();
                break;
                */
                /*
                ** Fall through
                 */

            case DELETE_CHUNKS_FROM_STORAGE_SERVERS:
                getServers(chunks, servers);

                if (servers.size() > 0) {
                    /*
                    ** Once all the requests are sent to the Storage Servers, then wait in the SET_CHUNKS_TO_AVAILABLE
                    **   state for all the results.
                     */
                    currState = ExecutionState.SET_CHUNKS_TO_AVAILABLE;

                    /*
                    ** Send the request to each server
                     */
                    for (ServerIdentifier server: servers) {
                        StorageServerDeleteChunkParams params = new StorageServerDeleteChunkParams(server);
                        params.setOpcClientRequestId(requestContext.getHttpInfo().getOpcClientRequestId());

                        HttpResponseInfo httpInfo = new HttpResponseInfo(requestContext.getRequestId());
                        server.setHttpInfo(httpInfo);

                        SendRequestToService cmdSend = new SendRequestToService(requestContext, memoryManager, server, params, null, this);
                        cmdSend.initialize();

                        serviceRequests.add(cmdSend);
                        cmdSend.event();
                    }
                } else {
                    String failureMessage = "{\r\n  \"code\":" + HttpStatus.NO_CONTENT_204 +
                            "\r\n  \"message\": \"No servers found\"" +
                            "\r\n}";
                    requestContext.getHttpInfo().emitMetric(HttpStatus.NO_CONTENT_204, failureMessage);
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.NO_CONTENT_204);

                    currState = ExecutionState.CALLBACK_OPS;
                    event();
                }
                break;

            case SET_CHUNKS_TO_AVAILABLE:
                boolean allResponded = true;
                for (ServerIdentifier server : servers) {
                    if (server.getResponseStatus() == -1) {
                        allResponded = false;
                        break;
                    }
                }

                if (allResponded) {
                    LOG.info("DeleteChunks() SET_CHUNKS_TO_AVAILABLE allResponded");

                    /*
                    ** Need to call complete() for all of the serviceRequests to make sure all the resources are
                    **   are cleaned up.
                     */
                    for (SendRequestToService cmdSend: serviceRequests) {
                        cmdSend.complete();
                    }
                    serviceRequests.clear();

                    /*
                    ** Now check the results for the different servers and if the results was OK_200 then move the
                    **   chunk status to CHUNK_AVAILABLE.
                    **
                    ** TODO: If the result from the status indicated that the chunk was not removed, there needs to be
                    **   a background thread that attempts to delete the chunk at a later point
                     */
                    for (ServerIdentifier server : servers) {
                        int result = requestContext.getStorageResponseResult(server);

                        if (result == HttpStatus.OK_200) {
                            chunkMgr.updateChunkStatus(server.getChunkUID(), ChunkStatusEnum.CHUNK_AVAILABLE);
                        } else {
                            chunkMgr.updateChunkStatus(server.getChunkUID(), ChunkStatusEnum.CHUNK_DELETE_FAILED);

                            String message = "{\r\n  \"code\":" + HttpStatus.UNPROCESSABLE_ENTITY_422 + "\n" +
                                    "  \"message\": \"StorageServerChunkDelete failed\"\n" +
                                    "  \"" + ContentParser.SERVER_IP + "\": \"" + server.getServerIpAddress().toString() + "\"\n" +
                                    "  \"" + ContentParser.SERVER_PORT + "\": \"" + server.getServerTcpPort() + "\"\n" +
                                    "  \"" + ContentParser.CHUNK_UID + "\": \"" + server.getChunkUID() + "\"\n" +
                                    "}";
                            requestContext.getHttpInfo().emitMetric(HttpStatus.UNPROCESSABLE_ENTITY_422, message);
                        }
                    }

                    servers.clear();

                    currState = ExecutionState.CALLBACK_OPS;
                    /*
                    ** Fall through
                     */
                } else {
                    /*
                    ** Still waiting on responses from the various Storage Servers
                     */
                    break;
                }

            case CALLBACK_OPS:
                currState = ExecutionState.EMPTY_STATE;

                chunks.clear();
                complete();
                completeCallback.complete();
                break;

            case EMPTY_STATE:
                break;
        }
    }

    public void complete() {
        LOG.info("DeleteChunks complete");
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
        //LOG.info("DeleteChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("DeleteChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("DeleteChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("DeleteChunks[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("DeleteChunks[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
    }

    /*
     ** This builds the OK_200 response headers for the DeleteChunks method (DELETE). This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     */
    private String buildSuccessHeader() {
        String successHeader;

        HttpRequestInfo requestInfo = requestContext.getHttpInfo();
        String opcClientRequestId = requestInfo.getOpcClientRequestId();
        int opcRequestId = requestInfo.getRequestId();

        if (opcClientRequestId != null) {
            successHeader = HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId + "\n" +
                            HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\n";
        } else {
            successHeader = HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId + "\n";
        }

        return successHeader;
    }

    /*
    ** This is used to obtain the Storage Server information needed to send the Chunk Delete request. The information
    **   needed is:
    **     service.serverIpAddress
    **     service.serverTcpPort
    **     service.chunkLBA
    **     service.chunkId
    **     service.chunkLocation (use the chunkUID for this)
     */
    private void getServers(final List<ChunkDeleteInfo> chunks, final List<ServerIdentifier> servers) {

        for (ChunkDeleteInfo chunk: chunks) {
            List<ServerIdentifier> foundServers = new LinkedList<>();

            requestContext.getServerTableMgr().getServer(chunk.getServerName(), foundServers);
            if (foundServers.size() == 1) {
                ServerIdentifier server = foundServers.get(0);

                server.setChunkUID(chunk.getChunkUID());
                server.setChunkLocation(chunk.getChunkUID());
                if (chunkMgr.getChunkInfoForDelete(server, chunk.getChunkUID())) {
                    servers.add(server);
                } else {
                    LOG.warn("Unable to locate chunkUID: " + chunk.getChunkUID());
                }
            }

            foundServers.clear();
        }
    }
}
