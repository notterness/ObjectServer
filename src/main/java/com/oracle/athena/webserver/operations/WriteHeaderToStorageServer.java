package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class WriteHeaderToStorageServer implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(WriteHeaderToStorageServer.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.WRITE_HEADER_TO_STORAGE_SERVER;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The IoInterface is what is used to communicate with the Storage Server.
     */
    private final IoInterface storageServerConnection;

    /*
     ** The following is the operation to run (if any) when the ConnectComplete is executed.
     */
    private final List<Operation> operationsToRun;

    /*
    ** This is the TCP Port of the Storage Server this header is being sent to
     */
    private final int storageServerTcpPort;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    private final BufferManager storageServerBufferManager;
    private final BufferManagerPointer writePointer;
    private BufferManagerPointer writeInfoPointer;
    private BufferManagerPointer writeDonePointer;

    /*
    ** Once the header has been written, this operation will call complete() to unregister the
    **   BufferManagerPointer(s).
     */
    private boolean completeCalled;

    public WriteHeaderToStorageServer(final RequestContext requestContext, final IoInterface storageServerConnection,
                                      final List<Operation> operationsToRun, final BufferManager storageServerBufferManager,
                                      final BufferManagerPointer writePtr, final int tcpPort) {

        this.requestContext = requestContext;
        this.storageServerConnection = storageServerConnection;
        this.operationsToRun = operationsToRun;

        this.storageServerBufferManager = storageServerBufferManager;
        this.writePointer = writePtr;

        this.storageServerTcpPort = tcpPort;

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        completeCalled = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
     */
    public BufferManagerPointer initialize() {
        /*
        ** This will be woken up when there is data to write via the writePointer. This is the dependency
        **   upon data to write.
         */
        writeInfoPointer = storageServerBufferManager.register(this, writePointer);

        /*
        ** This will be woken up when the data has been written out the SocketChannel via the writeDonePointer. This
        **   allows a check to insure that the data has been picked up and written to the SocketChannel. The
        **   writeInfoPointer is updated after the data has all been written out the SocketChannel. The update
        **   will cause the writeDonePointer to generate an event() for this operation. Then the check to make
        **   sure all the data was written can be done.
         */
        writeDonePointer = storageServerBufferManager.register(this, writeInfoPointer);

        /*
        ** Register with the storageServerConnection to perform the write. This tells the connection
        **   which BufferManager and the write BufferManagerPointer to use to get the ByteBuffers
        **   from.
         */
        storageServerConnection.registerWriteBufferManager(storageServerBufferManager, writeInfoPointer);

        return writeInfoPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
    ** The execute() method will be called:
    **   1) When BuildHeaderToStorageServer updates the writePointer that the writeInfoPointer has a dependency upon.
    **      This happens when the HTTP Request is written into the buffer.
    **   2) When the NioSocket performWrite() method has written all of the bytes in the ByteBuffer out the
    **      SocketChannel. When all the bytes have been written, then the writePointer is updated. The writeDonePointer
    **      has a dependency upon the writePointer and that will cause this operation to have its event() method
    **      called.
     */
    public void execute() {
        /*
        ** Check if there is data to write out or if it has all been written.
         */
        if (storageServerBufferManager.peek(writeInfoPointer) != null) {
            storageServerConnection.writeBufferReady();
        } else {
            /*
            ** Validate that this is being woken up and buffers have already been written to the
            **   SocketChannel
             */
            if (storageServerBufferManager.peek(writeDonePointer) != null) {
                /*
                 ** Check if the data has been written out. This means there are no buffers waiting to be written out
                 **   that are pointed to be the writeInfoPointer.
                 ** Set the HTTP Header has been written to the Storage Server flag
                 */
                LOG.info("WriteHeaderToStorageServer[" + requestContext.getRequestId() + "] all data written");
                requestContext.setHttpRequestSent(storageServerTcpPort);

                /*
                 ** event() all of the operations that are ready to run once the connect() has
                 **   succeeded.
                 */
                Iterator<Operation> iter = operationsToRun.iterator();
                while (iter.hasNext()) {
                    iter.next().event();
                }
                operationsToRun.clear();
            } else {
                LOG.info("WriteHeaderToStorageServer[" + requestContext.getRequestId() + "] waiting for HTTP header write");
            }
        }
    }

    /*
    ** This removes any dependencies that are put upon the BufferManager for this operation.
    ** At this point, all communications with the Storage Server for this chunk of data have been completed.
     */
    public void complete() {
        if (!completeCalled) {
            /*
            ** Remove the BufferManager registration from the NioSocket handler
             */
            storageServerConnection.unregisterWriteBufferManager();

            /*
             ** There is no longer a need for the writeDonePointer
             */
            storageServerBufferManager.unregister(writeDonePointer);
            writeDonePointer = null;

            /*
             ** Need to unregister() in the reverse order that the dependencies are created in
             */
            storageServerBufferManager.unregister(writeInfoPointer);
            writeInfoPointer = null;

            completeCalled = true;
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
