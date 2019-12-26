package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.manual.ClientTest;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.operations.ConnectComplete;
import com.oracle.athena.webserver.operations.Operation;
import com.oracle.athena.webserver.operations.OperationTypeEnum;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class SetupClientConnection implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(SetupClientConnection.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_CLIENT_CONNECTION;

    private final int WRITE_BUFFERS_TO_ALLOCATE = 10;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
    ** The ClientTest is used to know what data is to be filled into the ByteBuffers and sent to the
    **   WebServer
     */
    private final ClientTest clientTest;

    /*
     ** This is the IoInterface that the final status will be written out on.
     */
    private final IoInterface clientConnection;

    /*
    ** The targetTcpPort is where the connection is made to talk to the WebServer
     */
    private final int targetTcpPort;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
    ** Need a ConnectComplete and HandleInitiatorError operations for setting up the client initiator
    **   connection
     */
    private HandleInitiatorError initiatorError;
    private ClientConnectComplete connectComplete;

    private BufferManager clientWriteBufferManager;
    private BufferManagerPointer addBufferPointer;
    private BufferManagerPointer writeInfillPointer;
    private BufferManagerPointer writePointer;

    private ClientHttpHeaderWrite headerWrite;

    private final AtomicBoolean alreadyExecuted;

    public SetupClientConnection(final RequestContext requestContext, final ClientTest clientTest,
                                 final IoInterface connection, final int targetTcpPort) {

        this.requestContext = requestContext;
        this.clientTest = clientTest;
        this.clientConnection = connection;
        this.targetTcpPort = targetTcpPort;

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        this.alreadyExecuted = new AtomicBoolean(false);
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
    ** This will setup the TCP connection that the test will communicate with the WebServer over.
     */
    public BufferManagerPointer initialize() {

        /*
         ** Allocate buffers and add them to the clientWriteBufferManager
         */
        clientWriteBufferManager = requestContext.getClientWriteBufferManager();

        addBufferPointer = clientWriteBufferManager.register(this);
        clientWriteBufferManager.bookmark(addBufferPointer);

        ByteBuffer buffer;

        for (int i = 0; i < WRITE_BUFFERS_TO_ALLOCATE; i++) {
            buffer = ByteBuffer.allocate(MemoryManager.MEDIUM_BUFFER_SIZE);

            clientWriteBufferManager.offer(addBufferPointer, buffer);
        }
        clientWriteBufferManager.reset(addBufferPointer);

        writeInfillPointer = clientWriteBufferManager.register(this, addBufferPointer);

        /*
        ** When the ClientHttpHeaderWrite completes it then triggers the ClientObjectWrite, so
        **   the ClientObjectWrite needs to be passed into the ClientHttpHeaderWrite.
         */
        ClientObjectWrite objectWrite = new ClientObjectWrite(requestContext, clientConnection, clientTest,
                writeInfillPointer, targetTcpPort);
        objectWrite.initialize();

         /*
         ** Create the ClientHttpHeaderWrite operation and connect in this object to provide the HTTP header
         **   generator
         */
        headerWrite = new ClientHttpHeaderWrite(requestContext, clientConnection, clientTest,
                writeInfillPointer, objectWrite, targetTcpPort);
        headerWrite.initialize();

        /*
         ** Setup the Operation the will trigger the NIO work loop to actually perform the writes to
         **   the SocketChannel
         */
        ClientWrite clientWrite = new ClientWrite(requestContext, clientConnection, writeInfillPointer);
        writePointer = clientWrite.initialize();

        /*
         ** Register the writePointer and the clientWriteBufferManager with the
         **   IoInterface so it can actually write the data within the ByteBuffer(s) out
         **   the SocketChannel
         */
        clientConnection.registerWriteBufferManager(clientWriteBufferManager, writePointer);

        /*
        ** When the ConnectComplete event is called, this means that the writing of the HTTP Request can
        **   take place.
         */
        connectComplete = new ClientConnectComplete(requestContext, headerWrite, targetTcpPort, addBufferPointer);
        connectComplete.initialize();

        initiatorError = new HandleInitiatorError(requestContext, clientConnection);
        initiatorError.initialize();

        return writeInfillPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        if (!alreadyExecuted.get()) {
            requestContext.addToWorkQueue(this);
        }
     }

    /*
     */
    public void execute() {

        LOG.info("execute()");

        /*
        ** Only run this once, even though it will get kicked everytime a buffer is added to the
        **   available buffers. The better way would be to add a WriteBufferAdd operation or to
        **   allocate all of the buffers up front.
         */
        alreadyExecuted.set(true);

        /*
         ** Start the connection to the remote WebServer. When it completes, this Operation's event() method
         **   will be called and the ClientHttpHeaderWrite operation can be started.
         */
        clientConnection.startInitiator(InetAddress.getLoopbackAddress(), targetTcpPort, connectComplete, initiatorError);
    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {
        /*
        ** Walk the BufferManager freeing up all the allocated buffers
         */

        /*
        ** Unregister all of the BufferManagerPointer(s)
         */
        clientWriteBufferManager.unregister(addBufferPointer);
        clientWriteBufferManager.unregister(writeInfillPointer);

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
