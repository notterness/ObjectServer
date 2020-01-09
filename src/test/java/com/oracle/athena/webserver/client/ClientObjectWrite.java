package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.manual.ClientTest;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.operations.Operation;
import com.oracle.athena.webserver.operations.OperationTypeEnum;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.athena.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ClientObjectWrite implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ClientObjectWrite.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_OBJECT_WRITE;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** This is the IoInterface that the final status will be written out on.
     */
    private final IoInterface clientConnection;

    /*
    ** ClientTest provides the routines to know what HTTP Request and Object to send to the
    **   the WebServer and it provides the routines to validate the correct response from
    **   the WebServer.
     */
    private final ClientTest clientTest;

    private final BufferManagerPointer writeInfillPointer;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
     */
    private final ServerIdentifier serverIdentifier;

    public ClientObjectWrite(final RequestContext requestContext, final IoInterface connection,
                             final ClientTest clientTest, final BufferManagerPointer writeInfillPtr,
                             final ServerIdentifier serverIdentifier) {

        this.requestContext = requestContext;
        this.clientConnection = connection;
        this.clientTest = clientTest;
        this.writeInfillPointer = writeInfillPtr;
        this.serverIdentifier = serverIdentifier;

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
     */
    public BufferManagerPointer initialize() {

        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if the HTTP Request has been sent.
         */
        if (requestContext.hasHttpRequestBeenSent(serverIdentifier) == true) {
            requestContext.addToWorkQueue(this);
        }
    }

    /*
    ** This is only called once in this test
     */
    public void execute() {

        BufferManager clientWriteBufferMgr = requestContext.getClientWriteBufferManager();

        ByteBuffer buffer = clientTest.getObjectBuffer();

        if (buffer != null) {
            ByteBuffer infillBuffer = clientWriteBufferMgr.peek(writeInfillPointer);
            if (infillBuffer != null) {
                infillBuffer.put(buffer.array());
                infillBuffer.flip();

                clientWriteBufferMgr.updateProducerWritePointer(writeInfillPointer);
            } else {
                LOG.info("ClientWriteObject Infill Buffer null");
            }
        } else {
            LOG.info("ClientWriteObject Object Buffer null");
        }
    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {

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