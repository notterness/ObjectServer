package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.Md5Digest;
import com.webutils.webserver.common.Md5ResultHandler;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class ComputeMd5Digest implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ComputeMd5Digest.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.COMPUTE_MD5_DIGEST;

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
    private final BufferManager readBufferManager;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer md5DigestPointer;

    private final int clientObjectBytes;
    private int md5DigestedBytes;

    private final Md5Digest md5Digest;

    private final Md5ResultHandler resultUpdater;

    private int savedSrcPosition;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
    ** The simple constructor assumes that the BufferManager being used to compute the Md5 Digest is the incoming
    **   read buffer manager (the one that is created in the RequestContext). This constructor is used by:
    **      SetupStorageServerPut - This is used by the Storage Server to write chunks. It computes the Md5 on the
    **        chunk of data that is to be written.
    **      ObjectPut_P2 - The is part of the ObjectPut handler and is used to compute the Md5 checksum on the object
    **        data that is passed in by the client.
     */
    public ComputeMd5Digest(final RequestContext requestContext, final List<Operation> operationsToRun,
                            final BufferManagerPointer readBufferPtr, final Md5ResultHandler updater,
                            final int bytesToDigest) {

        this(requestContext, operationsToRun, readBufferPtr, requestContext.getClientReadBufferManager(), updater, bytesToDigest);
    }

    /*
    ** This constructor is used when the user wants to specify the BufferManager to use for buffers to compute the Md5
    **   digest on. This constructor is used by:
    **     SetupChunkRead - This is how the Object Server reads in chunks of data from the Storage Server. It computes
    **       the Md5 checksum on the data as it comes in and then compares it to the Md5 for the chunk that is saved in
    **       the objectStorageDb.storageChunk database table for the chunk being read in.
    **     WriteObjectChunk - This is what computes the Md5 digest on chunks as they are being sent to the Storage
    **       Servers.
    **     ClientGetObject - This computes the Md5 digest on the collection of chunks that make up a full object
    **       prior to them being sent to the client. This is part of the GetObject method handler.
     */
    public ComputeMd5Digest(final RequestContext requestContext, final List<Operation> operationsToRun,
                            final BufferManagerPointer readBufferPtr, final BufferManager readBufferMgr,
                            final Md5ResultHandler updater, final int bytesToDigest) {

        this.requestContext = requestContext;
        this.operationsToRun = operationsToRun;
        this.readBufferPointer = readBufferPtr;
        this.resultUpdater = updater;

        this.readBufferManager = readBufferMgr;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
         ** Setup the total number of bytes being transferred in this client object
         */
        clientObjectBytes = bytesToDigest;
        md5DigestedBytes = 0;
        savedSrcPosition = 0;

        md5Digest = new Md5Digest();
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

        md5DigestPointer = readBufferManager.register(this, readBufferPointer);

        /*
         ** savedSrcPosition is used to handle the case where there are multiple readers from the readBufferPointer and
         **   there has already been data read from the buffer. In that case, the position() will not be zero, but there
         **   is a race condition as to how the cursors within the "base" buffer are adjusted. The best solution is to
         **   use a "copy" of the buffer and to set its cursors appropriately.
         */
        ByteBuffer readBuffer;
        if ((readBuffer = readBufferManager.peek(md5DigestPointer)) != null) {
            savedSrcPosition = readBuffer.position();
            LOG.info("ComputeMd5Digest() savedSrcPosition: " + savedSrcPosition + " clientObjectBytes: " + clientObjectBytes);

            /*
             ** Add this to the execute queue since there is already data in a buffer to compute the Md5 digest for
             */
            event();
        } else {
            LOG.info("ComputeMd5Digest(1) savedSrcPosition: 0 clientObjectBytes: " + clientObjectBytes);
            savedSrcPosition = 0;
        }

        return md5DigestPointer;
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

        while ((buffer = readBufferManager.poll(md5DigestPointer)) != null) {

            /*
             ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
             **  affecting the position() and limit() indexes for other users of the base buffer
             */
            ByteBuffer md5Buffer = buffer.duplicate();
            md5Buffer.position(savedSrcPosition);
            savedSrcPosition = 0;

            LOG.info("md5 digest position: " + md5Buffer.position() + " limit: " + md5Buffer.limit());

            md5DigestedBytes += (md5Buffer.limit() - md5Buffer.position());
            md5Digest.digestByteBuffer(md5Buffer);

            if (md5DigestedBytes == clientObjectBytes) {
                /*
                ** Set the computed value at this point. The actual comparison is dependent upon if this is being
                **   computed for a request (in which case the "Content-Md5" value is passed in prior to any processing
                **   taking place) or if this is for handling a chunk (where the "Content-Md5" value is returned in
                **   the response header and the check takes place after the response has been processed).
                 */
                resultUpdater.setMd5DigestComplete(md5Digest.getFinalDigest());

                /*
                ** Everything is done for the Md5 Digest calculation and comparison.
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
        LOG.info("ComputeMd5Digest(" + requestContext.getRequestId() + ") complete");

        /*
        ** Make sure this is not on the compute thread pool's execution queue since the
        **   BufferManagerPointer is going to be made invalid
         */
        requestContext.removeComputeWork(this);

        readBufferManager.unregister(md5DigestPointer);
        md5DigestPointer = null;

        /*
         ** event() all of the operations that are ready to run once the Md5 Digest has
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
        //LOG.info("ComputeMd5Digest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ComputeMd5Digest[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ComputeMd5Digest[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ComputeMd5Digest[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ComputeMd5Digest[" + requestContext.getRequestId() +
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
