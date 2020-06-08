package com.webutils.chunkmgr.operations;

import com.webutils.chunkmgr.http.AllocateChunksGetContent;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.mysql.ServerChunkMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ContextNotEmptyException;
import java.util.LinkedList;
import java.util.List;

public class AllocateChunks implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(AllocateChunks.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.ALLOCATE_CHUNKS;

    /*
     ** The following are used to build the response header
     */
    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "allocated-chunks: ";

    private final RequestContext requestContext;

    private final AllocateChunksGetContent allocateChunksContent;

    private final Operation completeCallback;

    /*
     ** Used to insure that the database operations to allocate the chunks do not get run multiple times.
     */
    private boolean chunksAllocated;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public AllocateChunks(final RequestContext requestContext, final AllocateChunksGetContent allocateChunksContent,
                          final Operation completeCb) {
        this.requestContext = requestContext;
        this.allocateChunksContent = allocateChunksContent;
        this.completeCallback = completeCb;

        chunksAllocated = false;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
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
        if (!chunksAllocated) {
            ServerIdentifierTableMgr serverTableMgr = requestContext.getServerTableMgr();

            StorageTierEnum tier = allocateChunksContent.getStorageTier();
            List<ServerIdentifier> servers = new LinkedList<>();
            int status = serverTableMgr.getOrderedStorageServers(servers, tier, allocateChunksContent.getObjectChunkNumber());
            if (status == HttpStatus.OK_200) {
                List<ServerIdentifier> allocatedServers = new LinkedList<>();

                int requiredChunks = tier.getRedundancy();

                int chunksAllocated = allocateChunksFromServers(servers, requiredChunks, allocatedServers);
                if (chunksAllocated > 0) {
                    requestContext.getHttpInfo().setResponseHeaders(buildSuccessHeader(chunksAllocated));
                    requestContext.getHttpInfo().setResponseContent(buildResponseBody(allocatedServers));
                } else {
                    requestContext.getHttpInfo().setResponseHeaders(buildSuccessHeader(0));
                }
            } else {
                LOG.warn("AllocateChunks failed status: " + status);
            }

            chunksAllocated = true;

            completeCallback.complete();
        }
    }

    public void complete() {
        LOG.info("AllocateChunks complete");
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
        //LOG.info("AllocateChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("AllocateChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("AllocateChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("AllocateChunks[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("AllocateChunks[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
    }

    /*
    ** Walk through the list of servers and allocate a chunk from each one until the required number are
    **   allocated.
     */
    private int allocateChunksFromServers(final List<ServerIdentifier> serversToAllocateFrom, final int redundancy,
                                          final List<ServerIdentifier> allocatedServers) {

        ServerChunkMgr chunkMgr = new ServerChunkMgr(requestContext.getWebServerFlavor());
        int allocatedChunks = 0;

        for (ServerIdentifier server : serversToAllocateFrom) {
            if (chunkMgr.allocateStorageChunk(server) == HttpStatus.OK_200) {
                allocatedServers.add(server);
                allocatedChunks++;
                if (allocatedChunks == redundancy) {
                    break;
                }
            }
        }

        return allocatedChunks;
    }

    /*
     ** This builds the OK_200 response headers for the POST CreateServer command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   allocated-chunks - The number of chunks returned in the response body
     */
    private String buildSuccessHeader(final int allocated) {
        String successHeader;

        HttpRequestInfo requestInfo = requestContext.getHttpInfo();
        String opcClientRequestId = requestInfo.getOpcClientRequestId();
        int opcRequestId = requestInfo.getRequestId();

        if (opcClientRequestId != null) {
            successHeader = SUCCESS_HEADER_1 + opcClientRequestId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n" +
                    SUCCESS_HEADER_3 + allocated + "\n";
        } else {
            successHeader = SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + allocated + "\n";
        }

        return successHeader;
    }

    /*
    ** The response body has the following format for each chunk that is allocated
    **
    ** NOTE: The chunkUID is used for the chunk location to guarantee that it is unique and to make it easier to find
    **   when the delete chunk method is implemented.
     */
    private String buildResponseBody(final List<ServerIdentifier> allocatedServers) {
        StringBuilder body = new StringBuilder();
        int chunkIndex = 0;

        body.append("{\n");
        for (ServerIdentifier server : allocatedServers) {
            body.append("  \"chunk-" + chunkIndex + "\":\n");
            body.append("    {\n");
            body.append("       \"" + ContentParser.SERVER_NAME + "\": \"" + server.getServerName() + "\"\n");
            body.append("       \"storage-id\": \"" + server.getServerId() + "\"\n");
            body.append("       \"storage-server-ip\": \"" + server.getServerIpAddress().toString() + "\"\n");
            body.append("       \"storage-server-port\": \"" + server.getServerTcpPort() + "\"\n");
            body.append("       \"chunk-id\": \"" + server.getChunkId() + "\"\n");
            body.append("       \"" + ContentParser.CHUNK_UID + "\": \"" + server.getChunkUID() + "\"\n");
            body.append("       \"chunk-lba\": \"" + server.getChunkLBA() + "\"\n");
            body.append("       \"chunk-location\": \"" + server.getChunkUID() + "\"\n");
            body.append("    }\n");

            chunkIndex++;
        }
        body.append("}\n");
        return body.toString();
    }
}
