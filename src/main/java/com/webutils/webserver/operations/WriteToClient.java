package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
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
    private final Operation finalOperationToRun;

    private final ServerIdentifier server;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    private BufferManager clientWriteBufferManager;
    private final BufferManagerPointer bufferInfillPointer;
    private BufferManagerPointer writePointer;
    private BufferManagerPointer writeDonePointer;

    public WriteToClient(final RequestContext requestContext, final IoInterface connection,
                         final Operation operationToRun, final BufferManagerPointer bufferInfillPtr,
                         final ServerIdentifier server) {

        this(requestContext, connection, operationToRun, requestContext.getClientWriteBufferManager(), bufferInfillPtr,
                server);
    }

    public WriteToClient(final RequestContext requestContext, final IoInterface connection,
                         final Operation operationToRun, final BufferManager clientWriteBufferMgr,
                         final BufferManagerPointer bufferInfillPtr, final ServerIdentifier server) {

        this.requestContext = requestContext;
        this.clientConnection = connection;
        this.finalOperationToRun = operationToRun;
        this.bufferInfillPointer = bufferInfillPtr;

        this.server = server;

        this.clientWriteBufferManager = clientWriteBufferMgr;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

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
            **   and from this server's perspective it is done IF all of the buffers have been filled.
            ** Done with this client connection as well since this is only being used to write the HTTP Response
             */
            LOG.info("WriteToClient allClientBuffersFilled: " + requestContext.getAllClientBuffersFilled());
            if (requestContext.getAllClientBuffersFilled()) {
                /*
                **
                 */
                if (server != null) {
                    requestContext.setHttpRequestSent(server);
                }

                finalOperationToRun.event();
            }
        }
    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {
        clientConnection.unregisterWriteBufferManager();
        clientConnection = null;

        clientWriteBufferManager.unregister(writeDonePointer);
        writeDonePointer = null;

        clientWriteBufferManager.unregister(writePointer);
        writePointer = null;

        clientWriteBufferManager = null;
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
        //LOG.info("WriteToClient[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("WriteToClient[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("WriteToClient[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("WriteToClient[" + requestContext.getRequestId() + "] markAddToQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
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
        LOG.warn("WriteToClient[" + requestContext.getRequestId() +
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
