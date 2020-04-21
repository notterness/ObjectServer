package com.webutils.storageserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SetupStorageServerPut implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupStorageServerPut.class);

    /*
    ** The following are the Error Types that this Mock Storage Server can inject into the
    **   responses.
    **
     */
    private static final String CLOSE_CONNECTION_AFTER_HEADER = "DisconnectAfterHeader";

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_STORAGE_SERVER_PUT;

    private final RequestContext requestContext;

    private final Operation metering;

    /*
    ** The completeCallback will cause the final response to be sent out.
     */
    private final Operation completeCallback;

    private BufferManagerPointer clientReadPtr;


    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** There are two operations required to read data out of the clientReadBufferMgr and process it
     **   The Md5 Digest and the Encryption operations.
     **
     ** The following is a map of all of the created Operations to handle this request.
     */
    private Map<OperationTypeEnum, Operation> storageServerPutHandlerOperations;

    /*
    **
     */
    private boolean skipSendingStatus;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the V2 PUT
     **   request.
     */
    public SetupStorageServerPut(final RequestContext requestContext, final Operation metering, final Operation completeCb) {

        this.requestContext = requestContext;
        this.metering = metering;
        this.completeCallback = completeCb;

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        storageServerPutHandlerOperations = new HashMap<>();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
        **
         */
        skipSendingStatus = false;
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
        clientReadPtr = requestContext.getReadBufferPointer();

        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {
        /*
        ** Check if there is some error to be injected into the test
         */
        String errorType = requestContext.getHttpInfo().getTestType();
        if (errorType == null) {
            /*
             ** For the current test Storage Server implementation, simply write the bytes to a file and when
             **   that completes, call this Operation's complete() method.
             */
            WriteToFile writeToFile = new WriteToFile(requestContext, clientReadPtr, this);
            storageServerPutHandlerOperations.put(writeToFile.getOperationType(), writeToFile);
            writeToFile.initialize();

            /*
             ** Dole out another buffer to read in the content data if there is not data remaining in
             **   the buffer from the HTTP Parsing. This is only done once and all further doling out
             **   of buffers is done within the WriteToFile operation.
             */
            BufferManager clientReadBufferManager = requestContext.getClientReadBufferManager();
            ByteBuffer remainingBuffer = clientReadBufferManager.peek(clientReadPtr);
            if (remainingBuffer != null) {
                if (remainingBuffer.remaining() > 0) {
                    writeToFile.event();
                } else {
                    metering.event();
                }
            } else {
                /*
                 ** There are no buffers waiting with data, so need to dole out another buffer to start a read
                 **   operation.
                 */
                metering.event();
            }

            LOG.info("SetupStorageServerPut[" + requestContext.getRequestId() + "] initialized");
        } else {
            /*
            ** Need to perform whatever is requested by the errorType
             */
            if (errorType.compareTo(CLOSE_CONNECTION_AFTER_HEADER) == 0) {
                skipSendingStatus = true;
                complete();
            }
        }
    }

    /*
    ** This complete() is called when the WriteToFile operation has written all of its buffers
    **   to the file.
     */
    public void complete() {

        LOG.info("SetupStorageServerPut[" + requestContext.getRequestId() + "] complete()");

        /*
        ** Call the complete() method for any operations that this one created.
         */
        Collection<Operation> createdOperations = storageServerPutHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        storageServerPutHandlerOperations.clear();

        if (!skipSendingStatus) {
            completeCallback.event();
        } else {
            /*
            ** This is only for test error conditions. Go right to the cleanup for this request
             */
            requestContext.cleanupServerRequest();
        }

        LOG.info("SetupStorageServerPut[" + requestContext.getRequestId() + "] completed");
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
        //LOG.info("SetupStorageServerPut[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SetupStorageServerPut[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SetupStorageServerPut[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SetupStorageServerPut[" + requestContext.getRequestId() + "] markAddToQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
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

        Collection<Operation> createdOperations = storageServerPutHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
