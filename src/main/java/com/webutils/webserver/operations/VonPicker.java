package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class VonPicker implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(VonPicker.class);

    private static final int NUMBER_TEST_STORAGE_SERVERS = 2;

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.VON_PICKER;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following is the operation to run (if any) when the VON Pick has completed.
     */
    private List<Operation> operationsToRun;

    /*
    ** The following identifies the chunk being written to the Storage Servers. The chunk number will
    **   start at 0 and increment per 128MB being written.
     */
    private final int chunkNumber;

    private final int chunkBytesToWrite;

    private final MemoryManager memoryManager;

    private BufferManagerPointer storageServerWritePointer;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
    ** The following is a list of the Storage Servers that need to be written to.
     */
    private List<ServerIdentifier> serverList;

    private boolean storageServersIdentified;

    public VonPicker(final RequestContext requestContext, final List<Operation> operationsToRun,
                     final int chunkNumber, final MemoryManager memoryManager,
                     final BufferManagerPointer writePointer, final int bytesToWrite) {

        this.requestContext = requestContext;
        this.operationsToRun = operationsToRun;
        this.chunkNumber = chunkNumber;

        this.memoryManager = memoryManager;
        this.storageServerWritePointer = writePointer;

        this.chunkBytesToWrite = bytesToWrite;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        serverList = new LinkedList<>();
        storageServersIdentified = false;
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
     */
    public void execute() {
        if (!storageServersIdentified) {
            /*
            ** Call the VON Picker - This is a blocking operation and will take time. For the test purposes,
            **   add 3 Storage Servers
             */
            DbSetup dbSetup = requestContext.getDbSetup();
            if (dbSetup != null) {

                if (!dbSetup.getStorageServers(serverList, chunkNumber)) {
                    LOG.warn("Unable to obtain storageServerInfo chunk: " + chunkNumber);
                    for (ServerIdentifier serverIdentifier : serverList) {
                        LOG.warn("  serverIdentifier " + serverIdentifier.getServerIpAddress().getHostAddress() +
                                " port: " + serverIdentifier.getServerTcpPort());
                    }
                    return;
                }

                /*
                 ** For all of the Storage Servers start a Chunk write sequence
                 */
                int i = 0;
                for (ServerIdentifier serverIdentifier : serverList) {
                    SetupChunkWrite setupChunkWrite = new SetupChunkWrite(requestContext, serverIdentifier,
                            memoryManager, storageServerWritePointer, chunkBytesToWrite, this, i);
                    setupChunkWrite.initialize();
                    setupChunkWrite.event();

                    i++;
                }

                storageServersIdentified = true;
            } else {
                LOG.error("DbSetup is null, unable to execute VonPicker");
            }
        } else {
            /*
            ** Check if all of the Storage Servers have responded or have had an error (either a bad
            **   Shaw-256 validation, timed out, or disconnected).
             */
            boolean allResponded = true;
            for (ServerIdentifier serverIdentifier : serverList) {
                if (!requestContext.hasStorageServerResponseArrived(serverIdentifier)) {
                    allResponded = false;
                    break;
                }
            }

            if (allResponded) {

                /*
                ** For debug purposes, dump out the response results from the Storage Servers
                 */
                for (ServerIdentifier serverIdentifier : serverList) {
                    int result = requestContext.getStorageResponseResult(serverIdentifier);
                    LOG.info("ChunkWriteComplete addr: " + serverIdentifier.getServerIpAddress().toString() +
                            " port: " + serverIdentifier.getServerTcpPort() + " chunkNumber: " + chunkNumber +
                            " result: " + result);
                }

                /*
                ** Done so cleanup
                 */
                serverList.clear();

                /*
                 ** event() all of the operations that are ready to run once the VON Pick has
                 **   succeeded.
                 */
                for (Operation operation : operationsToRun) {
                    operation.event();
                }
                operationsToRun.clear();
            }
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
            LOG.error("requestId[" + requestContext.getRequestId() + "] VonPicker should never be on the timed wait queue");
        } else {
            onExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    /*
    ** VonPicker will never be on the timed wait queue
     */
    public boolean isOnTimedWaitQueue() {
        return false;
    }

    /*
    ** hasWaitTimeElapsed() should never be called for the VonPicker as it will execute as quickly as it can
     */
    public boolean hasWaitTimeElapsed() {
        LOG.error("requestId[" + requestContext.getRequestId() + "] VonPicker should never be on the timed wait queue");
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
