package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WriteToClient implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(WriteToClient.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.WRITE_TO_CLIENT;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    private IoInterface clientConnection;

    /*
     ** The following is the operation to run (if any) when the ConnectComplete is executed.
     */
    private Operation finalOperationToRun;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    private final BufferManager clientWriteBufferManager;
    private final BufferManagerPointer bufferInfillPointer;
    private BufferManagerPointer writePointer;
    private BufferManagerPointer writeDonePointer;

    public WriteToClient(final RequestContext requestContext, final IoInterface connection,
                         final Operation operationToRun, final BufferManagerPointer bufferInfillPtr) {

        this.requestContext = requestContext;
        this.clientConnection = connection;
        this.finalOperationToRun = operationToRun;
        this.bufferInfillPointer = bufferInfillPtr;

        this.clientWriteBufferManager = requestContext.getClientWriteBufferManager();

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
        /*
        ** The writePointer BufferManagerPointer is used to event() this operation when
        **   data is placed where the bufferInfillPointer is pointing and then the
        **   bufferInfillPointer is updated (incremented).
         */
        writePointer = clientWriteBufferManager.register(this, bufferInfillPointer);

        writeDonePointer = clientWriteBufferManager.register(this, writePointer);

        /*
         ** Register with the IoInterface to perform writes to the client
         */
        clientConnection.registerWriteBufferManager(clientWriteBufferManager, writePointer);

        return writePointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
    ** TODO: There needs to be another Operation or something to handle when the write is actually complete. This
    **   works if there is only a write of a header, but not for writing data back to the client.
     */
    public void execute() {
        /*
        ** The following is to check if there is data ready to be written to the client.
         */
        if (clientWriteBufferManager.peek(writePointer) != null) {
            clientConnection.writeBufferReady();
        }

        /*
        ** The following is to check if data has been written to the client (meaning there
        **   are buffers available that are dependent upon the writePointer).
         */
        if (clientWriteBufferManager.poll(writeDonePointer) != null) {
            /*
            ** Since there are "buffers" available, it means the data was written out the SocketChannel
            **   and from this server's perspective it is done.
             */

            /*
            ** Done with this client connection as well since this is only being used to write the HTTP Response
             */
            finalOperationToRun.event();
        }

    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {
        clientConnection.unregisterWriteBufferManager();

        clientWriteBufferManager.unregister(writeDonePointer);
        writeDonePointer = null;

        clientWriteBufferManager.unregister(writePointer);
        writePointer = null;
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
