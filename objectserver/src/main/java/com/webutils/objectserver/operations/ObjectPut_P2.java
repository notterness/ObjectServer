package com.webutils.objectserver.operations;

import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.Md5ResultHandler;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.ComputeMd5Digest;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class ObjectPut_P2 implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectPut_P2.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.OBJECT_PUT_P2;

    private final ObjectServerRequestContext requestContext;

    private final MemoryManager memoryManager;

    private final Operation metering;

    private final Operation completeCallback;

    private final Md5ResultHandler updater;

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
    private final Map<OperationTypeEnum, Operation> putHandlerOps;

    private boolean setupMethodDone;

    /*
     ** This is used to setup the initial Operation dependencies required to handle the V2 PUT
     **   request.
     ** The completeCb will call the DetermineRequest operation's event() method when the V2 PUT completes.
     **   Currently, the V2 PUT is marked complete when all the V2 PUT object data is written to the Storage Servers
     **   and the Md5 Digest is computed and the comparison against the expected result done.
     */
    public ObjectPut_P2(final ObjectServerRequestContext requestContext, final MemoryManager memoryManager, final Operation metering,
                      final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.metering = metering;
        this.completeCallback = completeCb;

        this.updater = requestContext.getMd5ResultHandler();

        /*
         ** Setup the list of Operations currently used to handle the V2 PUT
         */
        putHandlerOps = new HashMap<>();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        setupMethodDone = false;
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
        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    public void execute() {
        if (!setupMethodDone) {
             /*
             ** Add compute MD5 and encrypt to the dependency on the ClientReadBufferManager read pointer.
             */
            BufferManagerPointer readBufferPointer = requestContext.getReadBufferPointer();

            EncryptBuffer encryptBuffer = new EncryptBuffer(requestContext, memoryManager, readBufferPointer,
                    this);
            putHandlerOps.put(encryptBuffer.getOperationType(), encryptBuffer);
            encryptBuffer.initialize();

            /*
             ** The ComputeMd5Digest needs to be completed before the SendFinalStatus operation can be woken up
             **   to perform its work.
             **   The SendFinalStatus is dependent upon all the data being written and the Md5 Digest
             **   having completed.
             */
            List<Operation> callbackList = new LinkedList<>();
            callbackList.add(this);

            ComputeMd5Digest computeMd5Digest = new ComputeMd5Digest(requestContext, callbackList, readBufferPointer,
                    updater, requestContext.getRequestContentLength());
            putHandlerOps.put(computeMd5Digest.getOperationType(), computeMd5Digest);
            computeMd5Digest.initialize();

            /*
             ** Dole out another buffer to read in the content data if there is not data remaining in
             **   the buffer from the HTTP Parsing.
             */
            BufferManager clientReadBufferManager = requestContext.getClientReadBufferManager();
            ByteBuffer remainingBuffer = clientReadBufferManager.peek(readBufferPointer);
            if (remainingBuffer != null) {
                if (remainingBuffer.remaining() > 0) {
                    encryptBuffer.event();
                } else {
                    metering.event();
                }
            }

            setupMethodDone = true;
        } else {
            /*
             ** Do nothing. The problem is this has a dependency upon the clientReadPtr
             */
        }
    }

    /*
     ** This is called from both the EncryptBuffer and ComputeMd5Digest operations when they have completed their
     **   work.
     */
    public void complete() {
        if (updater.getMd5DigestComplete() && requestContext.getAllPutDataWritten()) {
            /*
             ** Validate the Md5 digest. First read in the value from the Request Header and then compare it to the
             **   computed digest.
             **
             ** NOTE: The error status is updated within the method so nothing to do if it fails
             */
            updater.validateMD5Header();
            updater.checkContentMD5();

            completeCallback.event();

            putHandlerOps.clear();

            LOG.info("ObjectPut_P2[" + requestContext.getRequestId() + "] completed");
        } else {
            LOG.info("ObjectPut_P2[" + requestContext.getRequestId() + "] not completed digestComplete: " +
                    updater.getMd5DigestComplete() + " all data written: " + requestContext.getAllPutDataWritten());
        }
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
        //LOG.info("ObjectPut_P2[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ObjectPut_P2[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ObjectPut_P2[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ObjectPut_P2[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ObjectPut_P2[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        LOG.info("   -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = putHandlerOps.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
