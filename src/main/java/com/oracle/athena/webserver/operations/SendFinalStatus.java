package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.http.BuildHttpResult;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.requestcontext.RequestContext;
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
    ** The clientWriteBufferMgr is used to queue up writes back to the client. In this case, the
    **   writes are to send the final request status.
     */
    private BufferManager clientWriteBufferMgr;
    private BufferManagerPointer writeStatusBufferPtr;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (connOnDelayedQueue) or the normal
     **   execution queue (connOnExecutionQueue) is will always go on the execution queue.
     */
    private boolean connOnDelayedQueue;
    private boolean connOnExecutionQueue;
    private long nextExecuteTime;

    /*
    ** This is used to construct the final response to send to the client.
     */
    private BuildHttpResult resultBuilder;


    public SendFinalStatus(final RequestContext requestContext, final MemoryManager memoryManager) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;

        this.clientWriteBufferMgr = this.requestContext.getClientWriteBufferManager();

        this.resultBuilder = new BuildHttpResult();

        this.writeStatusBufferPtr = this.clientWriteBufferMgr.register(this);

        /*
         ** This starts out not being on any queue
         */
        connOnDelayedQueue = false;
        connOnExecutionQueue = false;
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

        return writeStatusBufferPtr;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
    ** This will attempt to allocate a ByteBuffer from the free pool and then fill it in with the
    **   response data.
    ** Assuming the B
     */
    public void execute() {

        int resultCode = requestContext.getHttpParseStatus();

        ByteBuffer respBuffer = memoryManager.poolMemAlloc(MemoryManager.MEDIUM_BUFFER_SIZE, null);
        if (respBuffer != null) {
            resultBuilder.buildResponse(respBuffer, resultCode, true, true);

            respBuffer.flip();

            HttpStatus.Code result = HttpStatus.getCode(resultCode);
            if (result != null) {
                LOG.info("SendFinalStatus[" + requestContext.getRequestId() + "] resultCode: " + result.getCode() + " " + result.getMessage());
            } else {
                LOG.info("SendFinalStatus[" + requestContext.getRequestId() + "] resultCode: " + result.getCode());
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
    }

    /*
    ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {

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
     **   markRemoveFromQueue - This method is used by the ServerWorkerThread to update the queue
     **     the connection is on when the connection is removed from the queue.
     **   markAddedToQueue - This method is used when a connection is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **
     ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
     **   of which queue the connection is on. It will probably clean up the code some.
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ConnectionState[" + connStateId + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (connOnDelayedQueue) {
            if (!delayedExecutionQueue) {
                LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
            }

            connOnDelayedQueue = false;
            nextExecuteTime = 0;
        } else if (connOnExecutionQueue){
            if (delayedExecutionQueue) {
                LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on workQueue");
            }

            connOnExecutionQueue = false;
        } else {
            LOG.warn("CloseOutRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            nextExecuteTime = System.currentTimeMillis() + TIME_TILL_NEXT_TIMEOUT_CHECK;
            connOnDelayedQueue = true;
        } else {
            connOnExecutionQueue = true;
        }
    }

    public boolean isOnWorkQueue() {
        return connOnExecutionQueue;
    }

    public boolean isOnTimedWaitQueue() {
        return connOnDelayedQueue;
    }

    public boolean hasWaitTimeElapsed() {
        long currTime = System.currentTimeMillis();

        if (currTime < nextExecuteTime) {
            return false;
        }

        //LOG.info("CloseOutRequest[" + requestContext.getRequestId() + "] waitTimeElapsed " + currTime);
        return true;
    }

}
