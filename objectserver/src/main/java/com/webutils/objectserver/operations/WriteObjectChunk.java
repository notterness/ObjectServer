package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.ResponseMd5ResultHandler;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerChunkMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.mysql.StorageChunkTableMgr;
import com.webutils.webserver.operations.ComputeMd5Digest;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class WriteObjectChunk implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(WriteObjectChunk.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.WRITE_OBJECT_CHUNK;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following is the operation to run (if any) when the Storage Server chunk allocation and the writes have
     **   all been completed.
     */
    private final List<Operation> callbackOperationsToRun;

    /*
     ** The following are used by the SetupChunkWrite operations to write out a chunk of data
     */
    private final int chunkNumber;
    private final int chunkBytesToWrite;
    private final MemoryManager memoryManager;
    private final BufferManagerPointer storageServerWritePointer;

    enum ExecutionState {
        REQUEST_STORAGE_CHUNK,
        STORAGE_CHUNK_INFO_SAVED,
        WRITE_STORAGE_CHUNK_COMPLETE,
        WRITE_STORAGE_CHUNK_DONE
    }

    private ExecutionState currState;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
    ** The following is a list of the Storage Servers that need to be written to.
     */
    private final List<ServerIdentifier> serverList;

    /*
    ** The following is the list of Operations that need to be called back when the StorageChunkAllocated operation
    **   completes.
     */
    private final List<Operation> storageChunkAllocatedCb;
    private StorageChunkAllocated storageChunkAllocated;

    /*
    ** The following are used to Parse the Http responses coming back from the Storage Servers
     */
    /*
     ** This is used to handle the compute of the Md5 for the chunk of data being sent to the Storage Server
     */
    private final ResponseMd5ResultHandler chunkMd5Updater;

    /*
    ** This Operation does two things, first it makes the call to allocate the chunks of storage on the various
    **   Storage Servers (this is based upon the storage class for the Object being written). Once the Storage chunk
    **   information has been obtained, then the StorageChunkAllocated operation will be run. The
    **   StorageChunkAllocated operation is responsible for writing the information about the chunk(s) to the
    **   database and associating it with the Object being written.
    **
    ** The following parameters are just used to pass information through to the SetupChunkWrite operation:
    **    memoryManager
    **    writePointer (storageServerWritePointer)
    **    bytesToWrite (chunkBytesToWrite)
     */
    public WriteObjectChunk(final RequestContext requestContext, final List<Operation> completionCbOpsToRun,
                            final int chunkNumber, final MemoryManager memoryManager,
                            final BufferManagerPointer writePointer, final int bytesToWrite) {

        this.requestContext = requestContext;
        this.callbackOperationsToRun = completionCbOpsToRun;

        this.chunkNumber = chunkNumber;
        this.memoryManager = memoryManager;
        this.storageServerWritePointer = writePointer;
        this.chunkBytesToWrite = bytesToWrite;

        this.chunkMd5Updater = new ResponseMd5ResultHandler(requestContext);

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        this.serverList = new LinkedList<>();

        this.storageChunkAllocatedCb = new LinkedList<>();

        currState = ExecutionState.REQUEST_STORAGE_CHUNK;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
     */
    public BufferManagerPointer initialize() {

        storageChunkAllocatedCb.add(this);

        storageChunkAllocated = new StorageChunkAllocated(requestContext, storageChunkAllocatedCb, serverList);
        storageChunkAllocated.initialize();

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

            case REQUEST_STORAGE_CHUNK:
                LOG.info("WriteObjectChunk() REQUEST_STORAGE_CHUNK");

                /*
                **
                 */
                try {
                    InetAddress inetAddress = InetAddress.getByName("localhost");
                    ServerIdentifier testServer = new ServerIdentifier("test-server", inetAddress, 5020, 0);
                    testServer.setServerId(2);
                    ServerChunkMgr chunkMgr = new ServerChunkMgr(requestContext.getWebServerFlavor());
                    chunkMgr.allocateChunk(testServer);
                } catch (UnknownHostException ex) {

                }

                /*
                 ** Call the Storage Chunk Picker - This is a blocking operation and will take time.
                 */
                ServerIdentifierTableMgr serverTableMgr = requestContext.getServerTableMgr();
                if (serverTableMgr != null) {
                    if (!serverTableMgr.getStorageServers(serverList, chunkNumber)) {
                        LOG.warn("Unable to obtain storageServerInfo chunk: " + chunkNumber);
                        for (ServerIdentifier serverIdentifier : serverList) {
                            LOG.warn("  serverIdentifier " + serverIdentifier.getServerIpAddress().getHostAddress() +
                                    " port: " + serverIdentifier.getServerTcpPort());
                        }
                    }

                    if (!serverList.isEmpty()) {
                        /*
                        ** Kick off the StorageChunkAllocated operation to save the information about the chunks that
                        **   are going to be written to the Storage Servers.
                         */
                        storageChunkAllocated.event();
                        currState = ExecutionState.STORAGE_CHUNK_INFO_SAVED;
                    } else {
                        /*
                        ** Need to set an error condition
                         */
                        currState = ExecutionState.WRITE_STORAGE_CHUNK_COMPLETE;
                        event();
                    }
                } else {
                    /*
                    ** Nothing more can be done, might as well return an error
                     */
                    currState = ExecutionState.WRITE_STORAGE_CHUNK_COMPLETE;
                    event();
                }
                break;

            case STORAGE_CHUNK_INFO_SAVED:
                LOG.info("WriteObjectChunk() STORAGE_CHUNK_INFO_SAVED");

                /*
                ** Start an Md5 digest for the chunk
                 */
                List<Operation> callbackList = new LinkedList<>();
                callbackList.add(this);

                ComputeMd5Digest chunkMd5Digest = new ComputeMd5Digest(requestContext, callbackList, storageServerWritePointer,
                        requestContext.getStorageServerWriteBufferManager(), chunkMd5Updater, chunkBytesToWrite);
                chunkMd5Digest.initialize();

                /*
                ** For all of the Storage Servers start a Chunk write sequence.
                **
                ** NOTE: The complete() function for SetupChunkWrite is called within that Operation.
                 */
                int i = 0;
                for (ServerIdentifier server : serverList) {
                    /*
                    ** FIXME: The setLength should be a passed in parameter to the allocation call for the obtaining
                    **   the ServerIdentifier
                     */
                    HttpResponseInfo httpInfo = new HttpResponseInfo(requestContext.getRequestId());
                    server.setHttpInfo(httpInfo);

                    server.setLength(chunkBytesToWrite);
                    SetupChunkWrite setupChunkWrite = new SetupChunkWrite(requestContext, server,
                            memoryManager, storageServerWritePointer, chunkBytesToWrite, this, i,
                            null);
                    setupChunkWrite.initialize();
                    setupChunkWrite.event();

                    i++;
                }

                currState = ExecutionState.WRITE_STORAGE_CHUNK_COMPLETE;
                break;

            case WRITE_STORAGE_CHUNK_COMPLETE:
                /*
                ** Verify that the Md5 computation has been completed.
                ** Check if all of the Storage Servers have responded or have had an error (either a bad
                **   Shaw-256 validation, timed out, or disconnected).
                 */
                boolean allResponded = chunkMd5Updater.getMd5DigestComplete();
                for (ServerIdentifier serverIdentifier : serverList) {
                    if (!requestContext.hasStorageServerResponseArrived(serverIdentifier)) {
                        allResponded = false;
                        break;
                    }
                }

                if (allResponded) {
                    LOG.info("WriteObjectChunk() WRITE_STORAGE_CHUNK_COMPLETE allResponded");

                    /*
                     ** For debug purposes, dump out the response results from the Storage Servers
                     */
                    HttpRequestInfo objectCreateInfo = requestContext.getHttpInfo();
                    StorageChunkTableMgr chunkMgr = new StorageChunkTableMgr(requestContext.getWebServerFlavor(), objectCreateInfo);

                    /*
                    ** There needs to be at least 1 chunk written to a Storage Server to continue
                     */
                    int chunkRedundancy = 0;
                    for (ServerIdentifier server : serverList) {
                        int result = requestContext.getStorageResponseResult(server);

                        /*
                        ** Verify the Md5 that was computed by the Storage Server
                         */
                        HttpResponseInfo httpInfo = server.getHttpInfo();


                        /*
                        ** If the status is OK_200, then update the chunk to mark that the data was written and save
                        **   the Md5 Digest for the chunk. The Md5 for the chunk is used to validate that data read
                        **   back from the Storage Server is valid.
                         */
                        if (result == HttpStatus.OK_200) {
                            if (chunkMgr.setChunkWritten(server.getChunkId(), chunkMd5Updater.getComputedMd5Digest())) {
                                LOG.info("ChunkWriteComplete addr: " + server.getServerIpAddress().toString() +
                                        " port: " + server.getServerTcpPort() + " chunkNumber: " + chunkNumber +
                                        " result: OK_200 " + chunkRedundancy);

                                chunkMgr.getChunkMd5Digest(server.getChunkId());
                                chunkRedundancy++;
                            } else {
                                LOG.error("ChunkWriteComplete unable to update dataWritten addr: " + server.getServerIpAddress().toString() +
                                        " port: " + server.getServerTcpPort() + " chunkNumber: " + chunkNumber +
                                        " result: OK_200");

                                chunkMgr.deleteChunk(server.getChunkId());
                            }
                        } else {
                            /*
                            ** Delete this chunk, hopefully this is not the lst
                             */
                            LOG.error("ChunkWriteComplete unable to update dataWritten addr: " + server.getServerIpAddress().toString() +
                                    " port: " + server.getServerTcpPort() + " chunkNumber: " + chunkNumber +
                                    " result: " + result);

                            chunkMgr.deleteChunk(server.getChunkId());
                        }

                        /*
                         ** Remove this serverIdentifier from the list
                         **
                         ** Need to remove the reference to the HttpResponseInfo from the server to insure it gets released
                         */
                        server.setHttpInfo(null);

                        requestContext.removeStorageServerResponse(server);
                    }

                    /*
                    ** Make sure that there was at least 1 valid chunk written
                     */
                    if (chunkRedundancy == 0) {
                        StringBuilder failureMessage = new StringBuilder("\"Unable to obtain write chunk data - failed Storage Servers\"");
                        for (ServerIdentifier server : serverList) {
                            failureMessage.append(",\n  \"StorageServer\": \"").append(server.getServerName()).append("\"");
                        }
                        objectCreateInfo.setParseFailureCode(HttpStatus.INTERNAL_SERVER_ERROR_500, failureMessage.toString());
                    }

                    /*
                     ** Done so cleanup the active list of Storage Servers
                     */
                    serverList.clear();

                    /*
                     ** event() all of the operations that are ready to run once the VON Pick has
                     **   succeeded.
                     */
                    for (Operation operation : callbackOperationsToRun) {
                        operation.event();
                    }
                    callbackOperationsToRun.clear();

                    currState = ExecutionState.WRITE_STORAGE_CHUNK_DONE;
                }
                break;

            case WRITE_STORAGE_CHUNK_DONE:
                break;
        }
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
