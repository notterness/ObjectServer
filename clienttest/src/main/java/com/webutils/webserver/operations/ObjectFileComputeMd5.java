package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.common.Md5Digest;
import com.webutils.webserver.common.PutObjectParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ObjectFileComputeMd5 implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectFileComputeMd5.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.OBJECT_FILE_COMPUTE_MD5;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following is the operation to run (if any) when the ComputeMd5Digest is executed.
     */
    private final Operation completionCbOpToRun;

    private final MemoryManager memoryManager;

    /*
     ** The readBufferPointer is used to track ByteBuffer(s) that are filled with client object data and are
     **   ready to have the their incremental Md5 Digest computed.
     ** The md5DigestPointer tracks the clientReadBufferManager where data is placed following reads from
     **   the client connection's SocketChannel.
     */
    private final BufferManager readFileBufferManager;
    private BufferManagerPointer md5DigestPointer;

    private long clientObjectBytes;
    private long md5DigestedBytes;

    private final Md5Digest md5Digest;

    private final PutObjectParams requestParams;

    private int savedSrcPosition;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> computeFileMd5Ops;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;


    public ObjectFileComputeMd5(final RequestContext requestContext, final Operation completionCbOpToRun,
                                final MemoryManager memoryManager, final PutObjectParams putRequestParams) {

        this.requestContext = requestContext;
        this.completionCbOpToRun = completionCbOpToRun;
        this.memoryManager = memoryManager;
        this.requestParams = putRequestParams;

        this.readFileBufferManager = requestContext.getClientWriteBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        md5DigestedBytes = 0;
        savedSrcPosition = 0;

        computeFileMd5Ops = new HashMap<>();

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

        /*
         ** Setup the total number of bytes in the file the client wants to upload to the object server
         */
        clientObjectBytes = requestParams.setObjectSizeInBytes();

        /*
         ** Allocate ByteBuffer(s) to read in the response from the Storage Server. By using a metering operation, the
         **   setup for the reading of the Storage Server response header can be be deferred until the TCP connection to the
         **   Storage Server is successful.
         */
        FileReadBufferMetering bufferMetering = new FileReadBufferMetering(requestContext, memoryManager);
        computeFileMd5Ops.put(bufferMetering.getOperationType(), bufferMetering);
        BufferManagerPointer readBufferPointer = bufferMetering.initialize();

        ReadObjectFromFile readFromFile = new ReadObjectFromFile(requestContext, bufferMetering, readBufferPointer,
                requestParams, this);
        computeFileMd5Ops.put(readFromFile.getOperationType(), readFromFile);
        BufferManagerPointer fileReadPointer = readFromFile.initialize();

        md5DigestPointer = readFileBufferManager.register(this, fileReadPointer);

        /*
        ** Start the reading from the file by metering out a buffer to read data into
         */
        bufferMetering.event();

        return md5DigestPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     ** This computes the Md5 Digest for ByteBuffer(s) that are read in for the client PUT object data.
     */
    public void execute() {
        if (!requestParams.getMd5DigestSet()) {
            ByteBuffer buffer;

            while ((buffer = readFileBufferManager.poll(md5DigestPointer)) != null) {

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
                    requestParams.setMd5Digest(md5Digest.getFinalDigest());

                    /*
                     ** Everything is done for the Md5 Digest calculation and comparison.
                     */
                    complete();
                    break;
                }
            }
        } else {
            LOG.info("ObjectFileComputeMd5 getMd5DigestSet() true");
        }
    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {
        LOG.info("ObjectFileComputeMd5 complete");

        /*
         ** Make sure this is not on the compute thread pool's execution queue since the
         **   BufferManagerPointer is going to be made invalid
         */
        requestContext.removeComputeWork(this);

        readFileBufferManager.unregister(md5DigestPointer);
        md5DigestPointer = null;

        /*
        ** Clean up any dependencies between the operations
         */
        Operation readFromFile = computeFileMd5Ops.remove(OperationTypeEnum.READ_OBJECT_FROM_FILE);
        readFromFile.complete();

        Operation meterBuffers = computeFileMd5Ops.remove(OperationTypeEnum.FILE_READ_BUFFER_METERING);
        meterBuffers.complete();

        Collection<Operation> createdOperations = computeFileMd5Ops.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }
        computeFileMd5Ops.clear();

        /*
         ** event() the operation that needs to run run once the Md5 Digest has been calculated.
         */
        completionCbOpToRun.event();
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
