package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.BuildHttpResult;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/*
** This is used to send the final status to the client.
 */
public class SendFinalStatus implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SendFinalStatus.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SEND_FINAL_STATUS;

    /*
    ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
    ** The MemoryManager is used to allocate a very small number of buffers used to send out the
    **   final status
     */
    private final MemoryManager memoryManager;

    /*
    ** This is the IoInterface that the final status will be written out on.
     */
    private final IoInterface clientConnection;

    /*
    ** The clientWriteBufferMgr is used to queue up writes back to the client. In this case, the
    **   writes are to send the final request status.
     */
    private BufferManager clientWriteBufferMgr;
    private BufferManagerPointer writeStatusBufferPtr;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
    ** This is used to construct the final response to send to the client.
     */
    private final BuildHttpResult resultBuilder;

    private boolean httpResponseSent;


    public SendFinalStatus(final RequestContext requestContext, final MemoryManager memoryManager,
                           final IoInterface connection) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.clientConnection = connection;

        this.clientWriteBufferMgr = this.requestContext.getClientWriteBufferManager();

        this.resultBuilder = new BuildHttpResult();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        httpResponseSent = false;
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
        writeStatusBufferPtr = this.clientWriteBufferMgr.register(this);

        /*
        ** Add a bookmark to insure that the pointer dependent upon this starts at
        **   position 0.
         */
        clientWriteBufferMgr.bookmark(writeStatusBufferPtr);

        return writeStatusBufferPtr;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
    ** This will allocate a ByteBuffer from the free pool and then fill it in with the HTTP Response data.
     */
    public void execute() {

        if (!httpResponseSent) {
            int resultCode = requestContext.getHttpParseStatus();

            ByteBuffer respBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, clientWriteBufferMgr,
                    operationType);
            if (respBuffer != null) {
                resultBuilder.buildResponse(respBuffer, resultCode, true, true);

                respBuffer.flip();

                HttpStatus.Code result = HttpStatus.getCode(resultCode);
                if (result != null) {
                    LOG.info("SendFinalStatus[" + requestContext.getRequestId() + "] resultCode: " + result.getCode() + " " + result.getMessage());
                } else {
                    LOG.info("SendFinalStatus[" + requestContext.getRequestId() + "] resultCode: INVALID (" + resultCode +")");
                }

                /*
                 ** Add the ByteBuffer to the clientWriteBufferMgr to kick off the write of the response to the client
                 */
                clientWriteBufferMgr.offer(writeStatusBufferPtr, respBuffer);

            } else {
                /*
                 ** If we are out of memory to allocate a response, might as well close out the connection and give up.
                 */
                LOG.info("SendFinalStatus[" + requestContext.getRequestId() + "] unable to allocate response buffer");

                /*
                 ** Go right to the CloseOutRequest operation. That will close out the connection.
                 */

            }

            httpResponseSent = true;
        } else {
            LOG.warn("SendFinalStatus[" + requestContext.getRequestId() + "] execute() after status sent");
        }
    }

    /*
    ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {

        /*
        ** Release any buffers in the clientWriteBufferMgr
         */
        int bufferCount = writeStatusBufferPtr.getCurrIndex();

        LOG.info("SendFinalStatus[" + requestContext.getRequestId() + "] complete() bufferCount: " + bufferCount);

        clientWriteBufferMgr.reset(writeStatusBufferPtr);
        for (int i = 0; i < bufferCount; i++) {
            ByteBuffer buffer = clientWriteBufferMgr.getAndRemove(writeStatusBufferPtr);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer, clientWriteBufferMgr);
            } else {
                LOG.info("SendFinalStatus[" + requestContext.getRequestId() + "] missing buffer i: " + i);
            }
        }

        clientWriteBufferMgr.unregister(writeStatusBufferPtr);
        writeStatusBufferPtr = null;

        clientWriteBufferMgr = null;
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
        //LOG.info("SendFinalStatus[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SendFinalStatus[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SendFinalStatus[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SendFinalStatus[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SendFinalStatus[" + requestContext.getRequestId() +
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
