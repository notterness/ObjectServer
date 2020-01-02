package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SetupV2Put implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupV2Put.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_V2_PUT;

    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    private final Operation metering;

    private final Operation completeCallback;

    private BufferManagerPointer clientReadPtr;
    private BufferManagerPointer encryptInputPointer;


    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
    ** There are two operations required to read data out of the clientReadBufferMgr and process it
    **   The Md5 Digest and the Encryption operations.
    **
    ** The following is a map of all of the created Operations to handle this request.
     */
    private Map<OperationTypeEnum, Operation> v2PutHandlerOperations;

    /*
    ** This is used to setup the initial Operation dependencies required to handle the V2 PUT
    **   request.
     */
    public SetupV2Put(final RequestContext requestContext, final MemoryManager memoryManager, final Operation metering,
                      final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.metering = metering;
        this.completeCallback = completeCb;

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        v2PutHandlerOperations = new HashMap<OperationTypeEnum, Operation>();

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
     ** This returns the BufferManagerPointer obtained by this operation, if there is one. If this operation
     **   does not use a BufferManagerPointer, it will return null.
     */
    public BufferManagerPointer initialize() {
        BufferManager clientReadBufferManager = requestContext.getClientReadBufferManager();
        clientReadPtr = requestContext.getReadBufferPointer();

        /*
        ** Need to create two pointer here that are clones of the clientReadPtr, one will be used by the
        **   Md5 Digest operation and the other will be used by the EncryptBuffer operation
         */
        encryptInputPointer = clientReadBufferManager.register(this, clientReadPtr);

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
        ** Add compute MD5 and encrypt to the dependency on the ClientReadBufferManager read pointer.
         */
        EncryptBuffer encryptBuffer = new EncryptBuffer(requestContext, memoryManager, encryptInputPointer,
                this);
        v2PutHandlerOperations.put(encryptBuffer.getOperationType(), encryptBuffer);
        encryptBuffer.initialize();

        /*
         ** Dole out another buffer to read in the content data if there is not data remaining in
         **   the buffer from the HTTP Parsing.
         */
        BufferManager clientReadBufferManager = requestContext.getClientReadBufferManager();
        ByteBuffer remainingBuffer = clientReadBufferManager.peek(clientReadPtr);
        if (remainingBuffer != null) {
            if (remainingBuffer.remaining() > 0) {
                encryptBuffer.event();
            } else {
                metering.event();
            }
        }
    }

    public void complete() {
        /*
        ** Remove the registration for the encryptInputPointer
         */
        BufferManager clientReadBufferManager = requestContext.getClientReadBufferManager();
        clientReadBufferManager.unregister(encryptInputPointer);
        encryptInputPointer = null;

        clientReadPtr = null;

        completeCallback.event();

        LOG.info("SetupV2Put[" + requestContext.getRequestId() + "] completed");

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
     ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
     **   of which queue the connection is on. It will probably clean up the code some.
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (onDelayedQueue) {
            if (!delayedExecutionQueue) {
                LOG.warn("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
            }

            onDelayedQueue = false;
            nextExecuteTime = 0;
        } else if (onExecutionQueue){
            if (delayedExecutionQueue) {
                LOG.warn("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on workQueue");
            }

            onExecutionQueue = false;
        } else {
            LOG.warn("requestId[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            nextExecuteTime = System.currentTimeMillis() + TIME_TILL_NEXT_TIMEOUT_CHECK;
            onDelayedQueue = true;
        } else {
            onExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return onExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return onDelayedQueue;
    }

    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        //LOG.info("requestId[" + requestContext.getRequestId() + "] waitTimeElapsed " + currTime);
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = v2PutHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
