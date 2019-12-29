package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class SetupChunkWrite implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SetupChunkWrite.class);

    private final int STORAGE_SERVER_HEADER_BUFFER_COUNT = 4;

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_CHUNK_WRITE;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    private final BufferManager clientWriteBufferMgr;
    private final BufferManagerPointer encryptedBufferPtr;

    private final int chunkBytesToEncrypt;

    private BufferManager storageServerBufferManager;

    /*
     ** The following is a map of all of the created Operations to handle this request.
     */
    private final Map<OperationTypeEnum, Operation> requestHandlerOperations;

    /*
    ** SetupChunkWrite is called at the beginning of each chunk (128MB) block of data. This is what sets
    **   up the calls to obtain the VON information and the meta-data write to the database.
     */
    public SetupChunkWrite(final RequestContext requestContext, final MemoryManager memoryManager,
                           final BufferManagerPointer encryptedBufferPtr,
                           final int chunkBytesToEncrypt) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.encryptedBufferPtr = encryptedBufferPtr;
        this.chunkBytesToEncrypt = chunkBytesToEncrypt;

        this.clientWriteBufferMgr = this.requestContext.getClientWriteBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        /*
         ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        requestHandlerOperations = new HashMap<OperationTypeEnum, Operation>();
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
     ** This returns the BufferManagerPointer obtained by this operation, if there is one. If this operation
     **   does not use a BufferManagerPointer, it will return null.
     */
    public BufferManagerPointer initialize() {
        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     **
     */
    public void execute() {

        /*
        ** Create a BufferManager with two required entries to send the HTTP Request header to the
        **   Storage Server and then to send the final Shaw-256 calculation.
         */
        storageServerBufferManager = new BufferManager(STORAGE_SERVER_HEADER_BUFFER_COUNT);

        /*
        ** Allocate ByteBuffer for the header and the Shaw-256
         */
        BufferManagerPointer addBufferPointer = storageServerBufferManager.register(this);
        storageServerBufferManager.bookmark(addBufferPointer);
        for (int i = 0; i < STORAGE_SERVER_HEADER_BUFFER_COUNT; i++) {
            ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, null);

            storageServerBufferManager.offer(addBufferPointer, buffer);
        }

        /*
        ** First determine the VON information for the various Storage Servers that need to be written to.
         */
        int storageServerTcpPort = RequestContext.STORAGE_SERVER_PORT_BASE;

        /*
        ** For each Storage Server, setup a HandleStorageServerError operation that is used when there
        **   is an error communicating with the StorageServer.
         */
        HandleStorageServerError errorHandler = new HandleStorageServerError(requestContext);
        requestHandlerOperations.put(errorHandler.getOperationType(), errorHandler);

        /*
         ** For each Storage Server, create the connection used to communicate with it.
         */
        IoInterface connection = requestContext.allocateConnection(this);

        /*
         ** For each Storage Server, create a WriteToStorageServer operation that will handle writing the data out
         **   to the Storage Server. The WriteToStorageServer will use the bookmark created in the
         **   EncryptBuffer operation to know where to start writing the data.
         */
        WriteToStorageServer storageServerWriter = new WriteToStorageServer(requestContext, connection,
                encryptedBufferPtr, storageServerTcpPort);
        requestHandlerOperations.put(storageServerWriter.getOperationType(), storageServerWriter);
        storageServerWriter.initialize();

        /*
        ** The PUT Header must be written to the Storage Server prior to sending the data
         */
        BuildHeaderToStorageServer headerBuilder = new BuildHeaderToStorageServer(requestContext, connection,
                storageServerBufferManager, addBufferPointer, chunkBytesToEncrypt);
        requestHandlerOperations.put(headerBuilder.getOperationType(), headerBuilder);
        BufferManagerPointer writePointer = headerBuilder.initialize();

        List<Operation> ops = new LinkedList<>();
        ops.add(storageServerWriter);
        WriteHeaderToStorageServer headerWriter = new WriteHeaderToStorageServer(requestContext, connection, ops,
                storageServerBufferManager, writePointer, storageServerTcpPort);
        requestHandlerOperations.put(headerWriter.getOperationType(), headerWriter);
        headerWriter.initialize();

        /*
         ** For each Storage Server, setup a ConnectComplete operation that is used when the NIO
         **   connection is made with the StorageServer.
         */
        List<Operation> operationList = new LinkedList<>();
        operationList.add(headerBuilder);
        ConnectComplete connectComplete = new ConnectComplete(requestContext, operationList,
                RequestContext.STORAGE_SERVER_PORT_BASE);
        requestHandlerOperations.put(connectComplete.getOperationType(), connectComplete);


        /*
        ** Now open a initiator connection to write encrypted buffers out of.
         */
        connection.startInitiator(InetAddress.getLoopbackAddress(), storageServerTcpPort,
                connectComplete, errorHandler);

    }

    /*
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
        LOG.info("  -> Operations Created By " + operationType);

        Collection<Operation> createdOperations = requestHandlerOperations.values();
        Iterator<Operation> iter = createdOperations.iterator();
        while (iter.hasNext()) {
            iter.next().dumpCreatedOperations(level + 1);
        }
        LOG.info("");
    }

}
