package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.Sha256Digest;
import com.webutils.webserver.common.Sha256ResultHandler;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class ComputeSha256Digest implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ComputeSha256Digest.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.COMPUTE_SHA256_DIGEST;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following is the operation to run (if any) when the ComputeMd5Digest is executed.
     */
    private final List<Operation> operationsToRun;

    /*
     ** The readBufferPointer is used to track ByteBuffer(s) that are filled with client object data and are
     **   ready to have the their incremental Md5 Digest computed.
     ** The md5DigestPointer tracks the clientReadBufferManager where data is placed following reads from
     **   the client connection's SocketChannel.
     */
    private final BufferManager clientReadBufferMgr;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer sha256DigestPointer;

    private final int clientObjectBytes;
    private int sha256DigestedBytes;

    private final Sha256ResultHandler updator;

    private final Sha256Digest sha256Digest;

    private int savedSrcPosition;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;


    public ComputeSha256Digest (final RequestContext requestContext, final List<Operation> operationsToRun,
                            final BufferManagerPointer readBufferPtr, final Sha256ResultHandler updator) {

        this.requestContext = requestContext;
        this.operationsToRun = operationsToRun;
        this.readBufferPointer = readBufferPtr;

        this.updator = updator;

        this.clientReadBufferMgr = this.requestContext.getClientReadBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         ** Setup the total number of bytes being transferred in this client object
         */
        clientObjectBytes = requestContext.getRequestContentLength();
        sha256DigestedBytes = 0;
        savedSrcPosition = 0;

        sha256Digest = new Sha256Digest();
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
     ** This sets up the Md5 Digest pointer with a dependency upon the read pointer. This way when new ByteBuffer(s)
     **   are filled, the Md5 Digest can be updated with the new buffers.
     */
    public BufferManagerPointer initialize() {

        sha256DigestPointer = clientReadBufferMgr.register(this, readBufferPointer);

        /*
         ** savedSrcPosition is used to handle the case where there are multiple readers from the readBufferPointer and
         **   there has already been data read from the buffer. In that case, the position() will not be zero, but there
         **   is a race condition as to how the cursors within the "base" buffer are adjusted. The best solution is to
         **   use a "copy" of the buffer and to set its cursors appropriately.
         */
        ByteBuffer readBuffer;
        if ((readBuffer = clientReadBufferMgr.peek(sha256DigestPointer)) != null) {
            savedSrcPosition = readBuffer.position();
        } else {
            savedSrcPosition = 0;
        }
        LOG.info("ComputeSha256Digest() savedSrcPosition: " + savedSrcPosition);

        return sha256DigestPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        //requestContext.addToWorkQueue(this);
        requestContext.runComputeWork(this);
    }

    /*
     ** This computes the Md5 Digest for ByteBuffer(s) that are read in for the client PUT object data.
     */
    public void execute() {
        ByteBuffer buffer;

        while ((buffer = clientReadBufferMgr.poll(sha256DigestPointer)) != null) {

            /*
             ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
             **  affecting the position() and limit() indexes for other users of the base buffer
             */
            ByteBuffer sha256Buffer = buffer.duplicate();
            sha256Buffer.position(savedSrcPosition);
            savedSrcPosition = 0;

            LOG.info("sha-256 digest position: " + sha256Buffer.position() + " limit: " + sha256Buffer.limit());

            sha256DigestedBytes += (sha256Buffer.limit() - sha256Buffer.position());
            sha256Digest.digestByteBuffer(sha256Buffer);

            if (sha256DigestedBytes == clientObjectBytes) {

                String sha256DigestString = sha256Digest.getFinalDigest();
                updator.setSha256DigestComplete(sha256DigestString);

                updator.checkContentSha256();

                /*
                 ** Everything is done for the Sha-256 Digest calculation and comparison.
                 */
                complete();
                break;
            }
        }
    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {
        LOG.info("ComputeSha256Digest(" + requestContext.getRequestId() + ") complete");

        /*
         ** Make sure this is not on the compute thread pool's execution queue since the
         **   BufferManagerPointer is going to be made invalid
         */
        requestContext.removeComputeWork(this);

        clientReadBufferMgr.unregister(sha256DigestPointer);
        sha256DigestPointer = null;

        /*
         ** event() all of the operations that are ready to run once the Sha-256 Digest has
         **   been calculated.
         */
        for (Operation operation : operationsToRun) {
            operation.event();
        }
        operationsToRun.clear();
    }

    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
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
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ComputeSha256Digest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ComputeSha256Digest[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ComputeSha256Digest[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ComputeSha256Digest[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ComputeSha256Digest[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
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
