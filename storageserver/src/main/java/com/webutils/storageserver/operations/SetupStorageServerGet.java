package com.webutils.storageserver.operations;

import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.Md5ResultHandler;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.CloseOutRequest;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.WriteToClient;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SetupStorageServerGet implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupStorageServerGet.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_STORAGE_SERVER_GET;

    /*
    ** The operations are all tied together via the RequestContext
     */
    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    /*
     ** The completeCallback will cause the final response to be sent out.
     */
    private final Operation completeCallback;

    /*
     ** There are two operations required to read data out of the clientReadBufferMgr and process it
     **   The Md5 Digest and the Encryption operations.
     **
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> storageServerGetHandlerOps;

    private final Md5ResultHandler updater;

    /*
     ** This is used to prevent the Operation setup code from being called multiple times in the execute() method
     */
    private boolean getOperationSetupDone;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the Storage Server GET
     **   request. This is how chunks of data for an Object are read to the backing storage.
     */
    public SetupStorageServerGet(final RequestContext requestContext, final MemoryManager memoryManager, final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.completeCallback = completeCb;

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        this.storageServerGetHandlerOps = new HashMap<>();

        this.updater = requestContext.getMd5ResultHandler();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         **
         */
        getOperationSetupDone = false;
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
    ** This sets up the following operations:
    **   BufferObjectGetMetering - this allocates the buffers used to send back the HTTP response headers and the
    **     object chunk's data.
    **   ReadFromFile - This takes the first buffer and builds the HTTP response headers into it. Once that is done,
    **      then it starts reading data from the chunk file into buffers.
    **   WriteToClient - This takes the filled in buffers and sends them to the Nio writer.
     */
    public void execute() {
        if (!getOperationSetupDone) {
            BufferObjectGetMetering metering = new BufferObjectGetMetering(requestContext, memoryManager);
            storageServerGetHandlerOps.put(metering.getOperationType(), metering);
            BufferManagerPointer meteringPtr = metering.initialize();

            ReadFromFile readFromFile = new ReadFromFile(requestContext, metering, meteringPtr, this);
            storageServerGetHandlerOps.put(readFromFile.getOperationType(), readFromFile);
            BufferManagerPointer objectDataReadyPtr = readFromFile.initialize();

            CloseOutRequest closeRequest = new CloseOutRequest(requestContext);
            storageServerGetHandlerOps.put(closeRequest.getOperationType(), closeRequest);
            closeRequest.initialize();

            WriteToClient writeToClient = new WriteToClient(requestContext, requestContext.getClientConnection(),
                    closeRequest, objectDataReadyPtr);
            storageServerGetHandlerOps.put(writeToClient.getOperationType(), writeToClient);

            /*
            ** Meter out a buffer to kick off the readFromFile operation (this will actually build the HTTP response
            **   buffer first and then start reading in data from the file).
             */
            metering.event();

            getOperationSetupDone = true;
        }

    }

    /*
     ** This complete() is called when the  operation has written all of its buffers
     **   to the file.
     */
    public void complete() {

        LOG.info("SetupStorageServerGet[" + requestContext.getRequestId() + "] complete()");

        /*
        ** Need to tear down the operations in the correct order to insure that the dependencies between the
        **   BufferManagerPointers are torn down correctly
         */
        Operation operation;

        operation = storageServerGetHandlerOps.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        operation.complete();

        operation = storageServerGetHandlerOps.remove(OperationTypeEnum.READ_FROM_FILE);
        operation.complete();

        operation = storageServerGetHandlerOps.remove(OperationTypeEnum.BUFFER_OBJECT_GET_METERING);
        operation.complete();

        /*
         ** Call the complete() method for any operations that this one created.
         */
        Collection<Operation> createdOperations = storageServerGetHandlerOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        storageServerGetHandlerOps.clear();

        completeCallback.event();

        LOG.info("SetupStorageServerGet[" + requestContext.getRequestId() + "] completed");
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
        //LOG.info("SetupStorageServerGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupStorageServerGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupStorageServerGet[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupStorageServerGet[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SetupStorageServerPut[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = storageServerGetHandlerOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }


}
