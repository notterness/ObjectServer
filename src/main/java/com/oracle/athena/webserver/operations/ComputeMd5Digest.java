package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.common.Md5Digest;
import com.oracle.athena.webserver.http.CasperHttpInfo;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class ComputeMd5Digest implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ComputeMd5Digest.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.COMPUTE_MD5_DIGEST;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following is the operation to run (if any) when the ComputeMd5Digest is executed.
     */
    private List<Operation> operationsToRun;

    /*
     ** The readBufferPointer is used to track ByteBuffer(s) that are filled with client object data and are
     **   ready to have the their incremental Md5 Digest computed.
     ** The md5DigestPointer tracks the clientReadBufferManager where data is placed following reads from
     **   the client connection's SocketChannel.
     */
    private final BufferManager clientReadBufferMgr;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer md5DigestPointer;

    private final int clientObjectBytes;
    private int md5DigestedBytes;

    private Md5Digest md5Digest;

    private final CasperHttpInfo casperHttpInfo;


    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;


    public ComputeMd5Digest(final RequestContext requestContext, final List<Operation> operationsToRun,
                           final BufferManagerPointer readBufferPtr) {

        this.requestContext = requestContext;
        this.operationsToRun = operationsToRun;
        this.readBufferPointer = readBufferPtr;

        this.clientReadBufferMgr = this.requestContext.getClientReadBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        /*
        ** Setup the total number of bytes being transferred in this client object
         */
        clientObjectBytes = requestContext.getRequestContentLength();
        md5DigestedBytes = 0;

        md5Digest = new Md5Digest();

        casperHttpInfo = requestContext.getHttpInfo();
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
    ** This sets up the Md5 Digest pointer with a dependency upon the read pointer. This way when new ByteBuffer(s)
    **   are filled, the Md5 Digest can be updated with the new buffers.
     */
    public BufferManagerPointer initialize() {

        md5DigestPointer = clientReadBufferMgr.register(this, readBufferPointer);

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
        ByteBuffer buffer;

        while ((buffer = clientReadBufferMgr.poll(md5DigestPointer)) != null) {

            LOG.info("md5 digest position: " + buffer.position() + " limit: " + buffer.limit());

            md5DigestedBytes += (buffer.limit() - buffer.position());
            md5Digest.digestByteBuffer(buffer);

            if (md5DigestedBytes == clientObjectBytes) {

                String dataDigestString = md5Digest.getFinalDigest();
                requestContext.setDigestComplete();

                boolean contentHasValidMd5Digest = casperHttpInfo.checkContentMD5(dataDigestString);
                LOG.info("WebServerConnState[" + requestContext.getRequestId() + "] Computed md5Digest " +
                        dataDigestString + " is valid: " + contentHasValidMd5Digest);
                requestContext.setMd5DigestCompareResult(contentHasValidMd5Digest);

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
        clientReadBufferMgr.unregister(md5DigestPointer);
        md5DigestPointer = null;

        /*
         ** event() all of the operations that are ready to run once the Md5 Digest has
         **   been calculated.
         */
        Iterator<Operation> iter = operationsToRun.iterator();
        while (iter.hasNext()) {
            iter.next().event();
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
        LOG.info("      No BufferManagerPointers");
        LOG.info("");
    }


}
