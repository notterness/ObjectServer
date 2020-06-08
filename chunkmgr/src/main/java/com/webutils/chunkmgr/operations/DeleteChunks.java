package com.webutils.chunkmgr.operations;

import com.webutils.chunkmgr.http.DeleteChunksContent;
import com.webutils.webserver.common.ChunkDeleteInfo;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.ChunkStatusEnum;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.mysql.ServerChunkMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
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

    /*
     ** The following are used to build the response header
     */
    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "ETag: ";

    private final RequestContext requestContext;

    private final DeleteChunksContent deleteChunksContent;

    private final Operation completeCallback;

    /*
     ** Used to insure that the database operations to create the bucket do not get run multiple times.
     */
    private boolean chunksDeleted;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public DeleteChunks(final RequestContext requestContext, final DeleteChunksContent deleteChunksContent,
                        final Operation completeCb) {
        this.requestContext = requestContext;
        this.deleteChunksContent = deleteChunksContent;
        this.completeCallback = completeCb;

        chunksDeleted = false;

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
        if (!chunksDeleted) {
            List<ChunkDeleteInfo> chunks = new LinkedList<>();
            deleteChunksContent.extractDeletions(chunks);

            LOG.info("chunks size() " + chunks.size());
            if (chunks.size() != 0) {
                ServerChunkMgr chunkMgr = new ServerChunkMgr(requestContext.getWebServerFlavor());

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
            }

            chunks.clear();
            completeCallback.complete();

            chunksDeleted = true;
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
     ** This builds the OK_200 response headers for the DELETE DeleteChunks method. This returns the following headers:
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
            successHeader = SUCCESS_HEADER_1 + opcClientRequestId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n";
        } else {
            successHeader = SUCCESS_HEADER_2 + opcRequestId + "\n";
        }

        return successHeader;
    }

}
