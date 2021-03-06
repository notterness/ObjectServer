package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class WriteToStorageServer implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(WriteToStorageServer.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.WRITE_TO_STORAGE_SERVER;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
    ** This is the connection used to write to the Storage Server
     */
    private IoInterface connection;

    /*
    ** This is the unique identifier for the write of the chunk to the Storage Server
     */
    private final ServerIdentifier serverIdentifier;

    private final BufferManager storageServerWriteBufferMgr;
    private final BufferManagerPointer encryptedBufferPtr;

    private final int bytesToWriteToStorageServer;

    private BufferManagerPointer writeToStorageServerPtr;
    private BufferManagerPointer writeDonePointer;

    private int bytesWrittenToStorageServer;

    private boolean registeredWriteBufferManager;
    private boolean completeCalled;

    public WriteToStorageServer(final RequestContext requestContext, final IoInterface connection,
                                final BufferManagerPointer encryptedBufferPtr, final int bytesToWrite,
                                final ServerIdentifier serverIdentifier) {

        this.requestContext = requestContext;
        this.connection = connection;
        this.encryptedBufferPtr = encryptedBufferPtr;
        this.bytesToWriteToStorageServer = bytesToWrite;
        this.serverIdentifier = serverIdentifier;

        this.storageServerWriteBufferMgr = this.requestContext.getStorageServerWriteBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        /*
        ** Keep track of the number of bytes actually written to the Storage Server so this operation knows when it
        **   has completed all of its work.
         */
        this.bytesWrittenToStorageServer = 0;

        /*
        ** Used to determine if this has been registered with the IoInterface to perform writes
         */
        this.registeredWriteBufferManager = false;

        /*
        ** Used to insure that complete() is not called multiple times
         */
        this.completeCalled = false;
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
        /*
         ** Register this with the Buffer Manager to allow it to be event(ed) when
         **   buffers are added by the EncryptBuffer producer
         */
        writeToStorageServerPtr = storageServerWriteBufferMgr.register(this, encryptedBufferPtr,
                RequestContext.getChunkSize());

        /*
        ** Register a dependency upon the writeToStorageServerPtr to know when the buffers have actually been
        **   written out to the SocketChannel.
         */
        writeDonePointer = storageServerWriteBufferMgr.register(this, writeToStorageServerPtr);

        return writeToStorageServerPtr;
    }

    /*
    ** This is the call when encrypted data is placed into the Storage Server Write BufferManager.
    **
    ** TODO: Make the connection.writeBufferReady() method thread safe so that it can be called from
    **   the event() method if desired. Then there is not another thread wakeup step to get to the
    **   execute() method.
     */
    public void event() {

        /*
         ** Add this to the execute queue if the HTTP Request has been sent to the Storage Server
         */
        if (requestContext.hasHttpRequestBeenSent(serverIdentifier)) {
            requestContext.addToWorkQueue(this);
        } else {
            LOG.info("WriteToStorageServer[" + requestContext.getRequestId() + "] HTTP request not sent");
        }
    }

    /*
     ** This just informs the IoInterface that there is at least one buffer with data in it that is
     **   ready to be written out the IoInterface.
     */
    public void execute() {
        if (!registeredWriteBufferManager) {
            /*
             ** Register this BufferManager and BufferManagerPointer with the IoInterface. This will only be used for
             **   writes out the IoInterface.
             */
            connection.registerWriteBufferManager(storageServerWriteBufferMgr, writeToStorageServerPtr);
            registeredWriteBufferManager = true;
        }

        if (storageServerWriteBufferMgr.peek(writeToStorageServerPtr) != null) {
            connection.writeBufferReady();
        } else {
            ByteBuffer buffer;
            while ((buffer = storageServerWriteBufferMgr.poll(writeDonePointer)) != null) {
                bytesWrittenToStorageServer += buffer.limit();
            }

            LOG.info("WriteToStorageServer[" + requestContext.getRequestId() + "] bytesWrittenToStorageServer: " +
                    bytesWrittenToStorageServer);
            if (bytesWrittenToStorageServer == bytesToWriteToStorageServer) {
                /*
                ** All the bytes have been written to this Storage Server, cleanup this operation
                 */
                complete();
            }
        }
    }

    /*
    ** This is called when all of the bytes for the chunk have been written to the Storage Server
     */
    public void complete() {
        if (!completeCalled) {
            /*
             ** Unregister the BufferManager and the BufferManagerPointer so that the IoInterface
             **   can be used cleanly by another connection later.
             ** In the case where the execute() method was not called (due to the inability to perform the initial
             **   connection to the Storage Server, the writeBufferManager will not have been registered.
             */
            if (registeredWriteBufferManager) {
                connection.unregisterWriteBufferManager();
            }

            storageServerWriteBufferMgr.unregister(writeDonePointer);
            writeDonePointer = null;

            storageServerWriteBufferMgr.unregister(writeToStorageServerPtr);
            writeToStorageServerPtr = null;

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
     **   markAddedToQueue - This method is used when an Operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     ** TODO: Might want to switch to using an enum instead of two different booleans to keep track
     **   of which queue the connection is on. It will probably clean up the code some.
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("WriteToStorageServer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("WriteToStorageServer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("WriteToStorageServer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("WriteToStorageServer[" + requestContext.getRequestId() + "] markAddToQueue(" +
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
        LOG.warn("WriteToStorageServer[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        writeToStorageServerPtr.dumpPointerInfo();
        LOG.info("");
    }


}
