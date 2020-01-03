package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.manual.ClientTest;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.operations.*;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.athena.webserver.requestcontext.ServerIdentifier;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SetupClientConnection implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(SetupClientConnection.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.SETUP_CLIENT_CONNECTION;

    private final int WRITE_BUFFERS_TO_ALLOCATE = 10;

    /*
    **
     */
    private final WebServerFlavor webServerFlavor;

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
    **
     */
    private final MemoryManager memoryManager;

    /*
    ** The ServerIdentifier contains the target server IP address and Port number for where the
    **   connection is made to talk to the WebServer
     */
    private final ServerIdentifier serverIdentifier;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
     ** The following is a map of all of the created Operations to handle this client request.
     */
    private Map<OperationTypeEnum, Operation> clientOperations;


    /*
    ** Need a ConnectComplete and HandleInitiatorError operations for setting up the client initiator
    **   connection
     */
    private HandleInitiatorError initiatorError;
    private ClientConnectComplete connectComplete;

    /*
    ** The following deal with sending data to the WebServer. This consists of allocating
    **   empty ByteBuffer(s), filling them in with the HTTP Request and the PUT Object
    **   data and then writing the data out the SocketChannel.
     */
    private BufferManager clientWriteBufferManager;
    private BufferManagerPointer addBufferPointer;
    private BufferManagerPointer writeInfillPointer;

    private final AtomicBoolean alreadyExecuted;

    public SetupClientConnection(final WebServerFlavor flavor, final RequestContext requestContext,
                                 final MemoryManager memoryManager, final ClientTest clientTest,
                                 final IoInterface connection, final ServerIdentifier serverIdentifier) {

        this.webServerFlavor = flavor;
        this.requestContext = requestContext;
        this.clientTest = clientTest;
        this.memoryManager = memoryManager;
        this.clientConnection = connection;
        this.serverIdentifier = serverIdentifier;

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        this.clientOperations = new HashMap<>();

        this.alreadyExecuted = new AtomicBoolean(false);
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
    ** This will setup the TCP connection that the test will communicate with the WebServer over.
    **
    ** This test uses only the ClientWriteBufferManager and ClientReadBufferManager to move
    **   ByteBuffer(s) around.
    **
    ** The dependencies for this are:
    **   ClientConnectComplete - This is executed when the connect() call completes in the NioSocket code. It
    **     is used to tell the rest of the state machine that there is a valid SocketChannel to read and write
    **     data from. When it executes, it sends an event to the ClientHttpRequestWrite() to start it
    **     running. It also, increments the addBufferPointer to "indicate" that a ByteBuffer is available to
    **     have data written into it. As a later optimization, this operation could be left out entirely and
    **     just have the ClientHttpRequestWrite be event(ed) directly when the connect() completes.
    **
    **   ClientHttpRequestWrite - This is what builds the HTTP Request via a call into the ClientTest
    **     child class that is actually running the test. The ClientHttpRequestWrite uses depends on
    **     the following BufferManagerPointers:
    **       -> addBufferPointer - This is used to add ByteBuffer(s) to the ClientWriteBufferManager. It
    **            is a producer.
    **       -> writeInfillPointer - This is used to access the ByteBuffer that is filled with data to be sent to
    **            the WebServer. It is used by the ClientHttpRequestWrite and ClientObjectWrite classes to
    **            obtain the buffer from the ClientWriteBufferManager. It is dependent upon the
    **            addBufferPointer.
    **
    **   ClientWriteObject - This executes after the ClientHttpRequestWrite has filled in the ByteBuffer (this is
    **     tracked via the requestContext.setHttpResponseSet(targetTcpPort) boolean) and there are buffers
    **     available (as added through the addBufferPointer). It uses the writeInfillPointer to obtain ByteBuffer(s)
    **     to put data in.
    **
    **   ClientWrite - This is what actually kicks the NioSocket code to indicate that there is data available to
    **     be written out the SocketChannel. It uses the following BufferManagerPointer:
    **       -> writePointer - This is the pointer to where the data is that is ready to be written out the
    **            SocketChannel. It is dependent upon the writeInfillPointer (that is the producer of the data
    **            to be written out and the writePointer is the consumer).
    **
    **   ClientReadResponse - This is what reads the HTTP Response from the WebServer in an processes it.
    **
     */
    public BufferManagerPointer initialize() {

        /*
         ** Allocate empty ByteBuffer(s) and add them to the clientWriteBufferManager
         */
        clientWriteBufferManager = requestContext.getClientWriteBufferManager();

        addBufferPointer = clientWriteBufferManager.register(this);
        clientWriteBufferManager.bookmark(addBufferPointer);

        for (int i = 0; i < WRITE_BUFFERS_TO_ALLOCATE; i++) {
            ByteBuffer buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, null);
            if (buffer != null) {
                clientWriteBufferManager.offer(addBufferPointer, buffer);
            } else {
                System.out.println("SetupClientConnection initialize() null buffer i: " + i);
            }
        }
        clientWriteBufferManager.reset(addBufferPointer);

        /*
        ** The writeInfillPointer is used to access empty ByteBuffer(s) that will be filled in
        **   with data to transfer to the WebServer
         */
        writeInfillPointer = clientWriteBufferManager.register(this, addBufferPointer);

        /*
        ** When the ClientHttpHeaderWrite completes it then triggers the ClientObjectWrite, so
        **   the ClientObjectWrite needs to be passed into the ClientHttpHeaderWrite.
         */
        ClientObjectWrite objectWrite = new ClientObjectWrite(requestContext, clientConnection, clientTest,
                writeInfillPointer, serverIdentifier);
        clientOperations.put(objectWrite.getOperationType(), objectWrite);
        objectWrite.initialize();

         /*
         ** Create the ClientHttpHeaderWrite operation and connect in this object to provide the HTTP header
         **   generator
         */
        ClientHttpRequestWrite headerWrite = new ClientHttpRequestWrite(requestContext, clientTest,
                writeInfillPointer, objectWrite, serverIdentifier);
        clientOperations.put(headerWrite.getOperationType(), headerWrite);
        headerWrite.initialize();

        /*
         ** Setup the Operation the will trigger the NIO work loop to actually perform the writes to
         **   the SocketChannel
         */
        ClientWrite clientWrite = new ClientWrite(requestContext, clientConnection, writeInfillPointer);
        clientOperations.put(clientWrite.getOperationType(), clientWrite);
        BufferManagerPointer writePointer = clientWrite.initialize();

        /*
         ** Register the writePointer and the clientWriteBufferManager with the
         **   IoInterface so it can actually write the data within the ByteBuffer(s) out
         **   the SocketChannel
         */
        clientConnection.registerWriteBufferManager(clientWriteBufferManager, writePointer);

        /*
        ** Now setup the operations required to read in the HTTP Response. Two of the operations,
        **   BufferReadMetering and ReadBuffer are already setup as part of the RequestContext since
        **   they are common operations.
        ** The readPointer from the RequestContext is required to allow the operation that will
        **   process the data to be able to access the data.
        **
        ** The allocation of empty ByteBuffer(s) is handled by the BufferReadMetering operation that is
        **   created and managed by the RequestContext.
        ** Setup the Metering and Read pointers since they are required for the HTTP Response Parser.
         */
        BufferReadMetering readMetering = new BufferReadMetering(webServerFlavor, requestContext, memoryManager);
        clientOperations.put(readMetering.getOperationType(), readMetering);
        BufferManagerPointer meteringPointer = readMetering.initialize();

        ReadBuffer readBuffer = new ReadBuffer(requestContext, meteringPointer, clientConnection);
        clientOperations.put(readBuffer.getOperationType(), readBuffer);
        BufferManagerPointer readPointer = readBuffer.initialize();

        ClientResponseHandler clientResponseHandler = new ClientResponseHandler(requestContext, clientTest, readPointer);
        clientOperations.put(clientResponseHandler.getOperationType(), clientResponseHandler);
        clientResponseHandler.initialize();

        /*
        ** When the ConnectComplete event is called, this means that the writing of the HTTP Request can
        **   take place.
         */
        List<Operation> operationsToRun = new LinkedList<>();
        operationsToRun.add(headerWrite);
        operationsToRun.add(readMetering);
        connectComplete = new ClientConnectComplete(requestContext, operationsToRun, addBufferPointer);
        clientOperations.put(connectComplete.getOperationType(), connectComplete);
        connectComplete.initialize();

        initiatorError = new HandleInitiatorError(requestContext, clientConnection);
        clientOperations.put(initiatorError.getOperationType(), initiatorError);
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
        clientConnection.startInitiator(serverIdentifier.getStorageServerIpAddress(),
                serverIdentifier.getStorageServerTcpPort(), connectComplete, initiatorError);
    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {

        /*
         ** Remove the BufferManager and BufferManagerPointer from the NioSocket (clientConnection)
         */
        clientConnection.unregisterWriteBufferManager();

        /*
        ** The following operations have dependencies upon each other so need to be completed() in the correct order.
        **
        ** METER_READ_BUFFERS provides the meteringPointer
        ** READ_BUFFER provides the readPointer that is dependent upon the meteringPointer
        ** CLIENT_RESPONSE_HANDLER creates the httpResponseBufferPointer which is dependent upon the readPointer
         */
        Operation operationToComplete;
        operationToComplete = clientOperations.remove(OperationTypeEnum.CLIENT_RESPONSE_HANDLER);
        operationToComplete.complete();

        operationToComplete = clientOperations.remove(OperationTypeEnum.READ_BUFFER);
        operationToComplete.complete();

        operationToComplete = clientOperations.remove(OperationTypeEnum.METER_READ_BUFFERS);
        operationToComplete.complete();

        /*
        ** Close out all of the remaining operations
         */
        Collection<Operation> createdOperations = clientOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.complete();
        }

        clientOperations.clear();

        /*
         ** Unregister all of the BufferManagerPointer(s). Since the writeInfillPointer has a dependency upon
         **   addBufferPointer, it must be unregistered() first.
         */
        clientWriteBufferManager.unregister(writeInfillPointer);

        /*
         ** Walk the BufferManager freeing up all the allocated buffers
         */
        clientWriteBufferManager.reset(addBufferPointer);
        for (int i = 0; i < WRITE_BUFFERS_TO_ALLOCATE; i++) {
            ByteBuffer buffer = clientWriteBufferManager.poll(addBufferPointer);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer);
            } else {
                System.out.println("ClientTest_CheckMd5 missing ByteBuffer i: " + i);
            }
        }

        clientWriteBufferManager.unregister(addBufferPointer);
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
