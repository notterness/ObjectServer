package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.mysql.StorageChunkTableMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
** This is called after the Storage Server and the "chunk" on the Storage Server has been picked and the record needs
**   to be created to associate the "chunk" with the Object. This operation creates the database record with the
**   information about the "chunk" so it can be accessed later.
 */
public class StorageChunkAllocated implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(StorageChunkAllocated.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.STORAGE_CHUNK_ALLOCATED;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;


    /*
     ** The following is the operation to run (if any) when the VON Pick has completed.
     */
    private final List<Operation> callbackOperationsToRun;

    private final List<ServerIdentifier> serverList;

    /*
    ** The following parameters are just used to pass information through to the SetupChunkWrite operation:
    **    memoryManager
    **    writePointer (storageServerWritePointer)
    **    bytesToWrite (chunkBytesToWrite)
     */
    public StorageChunkAllocated(final RequestContext requestContext, final List<Operation> completionCbOpsToRun,
                                 final List<ServerIdentifier> serverList) {

        this.requestContext = requestContext;
        this.callbackOperationsToRun = completionCbOpsToRun;
        this.serverList = serverList;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
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
    ** This writes the Storage Server chunk information out to the database for the Object. Once it completes that step,
    **   it events() the functions on the callbackOperationsToRun queue.
     */
    public void execute() {

        HttpRequestInfo objectCreateInfo = requestContext.getHttpInfo();
        int objectId = objectCreateInfo.getObjectId();

        /*
        ** If there is a problem with the objectId, no point in continuing.
         */
        if (objectId != -1) {
            StorageChunkTableMgr chunkMgr = new StorageChunkTableMgr(requestContext.getWebServerFlavor(), objectCreateInfo);

            /*
             ** For all of the Storage Servers start a Chunk write sequence
             */
            int status = HttpStatus.OK_200;

            for (ServerIdentifier server : serverList) {
                LOG.info("StorageChunkAllocated addr: " + server.getServerIpAddress().toString() +
                        " port: " + server.getServerTcpPort() + " chunkNumber: " + server.getChunkNumber());

                status = chunkMgr.createChunkEntry(objectId, server);
                if (status != HttpStatus.OK_200) {
                    break;
                }
            }

            if (status != HttpStatus.OK_200) {
                /*
                ** Need to delete any chunks and send an error
                 */
            }
        }

        /*
        ** event() all of the operations that are ready to run once the VON Pick has
        **   succeeded.
         */
        for (Operation operation : callbackOperationsToRun) {
            operation.event();
        }
        callbackOperationsToRun.clear();
    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {

    }


    /*
     ** The following are used to add the Operation to the event thread's event queue. To simplify the design an
     **   Operation can be added to the immediate execution queue or the delayed execution queue. An Operation
     **   cannot be on the delayed queue sometimes and on the work queue other times. Basically, an Operation is
     **   either designed to perform work as quickly as possible or wait a period of time and try again.
     **
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
        if (delayedExecutionQueue) {
            LOG.warn("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.error("requestId[" + requestContext.getRequestId() + "] WriteObjectChunk should never be on the timed wait queue");
        } else {
            onExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    /*
     ** WriteObjectChunk will never be on the timed wait queue
     */
    public boolean isOnTimedWaitQueue() {
        return false;
    }

    /*
     ** hasWaitTimeElapsed() should never be called for the WriteObjectChunk as it will execute as quickly as it can
     */
    public boolean hasWaitTimeElapsed() {
        LOG.error("requestId[" + requestContext.getRequestId() + "] WriteObjectChunk should never be on the timed wait queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("      No BufferManagerPointers");
        LOG.info("");
    }

}
