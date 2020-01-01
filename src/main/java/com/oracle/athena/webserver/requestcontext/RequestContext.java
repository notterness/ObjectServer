package com.oracle.athena.webserver.requestcontext;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.http.CasperHttpInfo;
import com.oracle.athena.webserver.http.HttpMethodEnum;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.EventPollThread;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.operations.*;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/*
** The RequestContext is how the lifecycle of an HTTP Request is handled. It contains the various state and
**   generated information for the request.
 */
public class RequestContext {

    private static final Logger LOG = LoggerFactory.getLogger(RequestContext.class);

    public static final int STORAGE_SERVER_PORT_BASE = 5010;

    /*
     ** This is the define for the chunk size in bytes. There are two different defines, one to allow easy testing of
     **   the chunk boundary crossing in a simulation environment and the other for production
     **
     **  MemoryManager.XFER_BUFFER_SIZE = 8k
     **  (MemoryManager.XFER_BUFFER_SIZE * 64) = 524,288
     **  (MemoryManager.XFER_BUFFER_SIZE * 16k) = 134,217,728 (128MB)
     */
    public final int TEST_CHUNK_SIZE_IN_BYTES = MemoryManager.XFER_BUFFER_SIZE * 64;
    public final int CHUNK_SIZE_IN_BYTES = MemoryManager.XFER_BUFFER_SIZE * 4096;


    private static final int BUFFER_MGR_RING_SIZE = 128;
    private static final int STORAGE_SERVER_RESPONSE_RING_SIZE = 10;

    private static final int MAX_EXEC_WORK_LOOP_COUNT = 10;

    /*
    ** This is the Chunk Size used
     */
    private final int chunkSize;


    /*
    ** A connection may have multiple requests within it. For example:
    **    GET, GET, GET
     */
    private int connectionRequestId;

    /*
    ** The IoInterface is used to read and write data from the client side. If this is in a production
    **   setup (and many test setups) the client side will be obtained through an NIO ServerSocketChannel
    **   accept() call and then the reads and writes will occur on an NIO SocketChannel.
    **
    ** For certain test cases, the IoInterface allows a file to be used to read data in from as well.
     */
    private IoInterface clientConnection;

    /*
    ** The WebServerFlavor is used to provide different limits and behavior for the Web Server
     */
    private final WebServerFlavor webServerFlavor;

    /*
    ** The memory manager used to populate the various BufferManagers
     */
    private final MemoryManager memoryManager;

    /*
    ** This is the thread this RequestContext will always run on. There are multiple RequestContext assigned
    **   to each thread.
     */
    private final EventPollThread threadThisContextRunsOn;


    /*
    ** This is the BufferManager to read data in from the client.
    **   The filled buffers may be the from the NIO layer, in which case the buffers are being filled through read
    **     calls on the SocketChannel.
    **   Or for one of the debug cases, the buffers are filled from a file read.
     */
    private final BufferManager clientReadBufferManager;

    /*
    ** This is the BufferManager used to write data out to the client.
     */
    private final BufferManager clientWriteBufferManager;

    /*
    ** This is the BufferManager used to hold the data being written out the Storage Servers. Since the same data is
    **   being written to all Storage Servers (at this point), there is a one to many relationship.
     */
    private final BufferManager storageServerWriteBufferManager;

    /*
    ** This is the BufferManager to receive responses from the StorageServers. There is one response BufferManager
    **   per StorageServer.
    ** TODO: This will need to be made into either an array or a Map to support more than one
    **   Storage Server.
     */
    private final BufferManager storageServerResponseBufferManager;

    /*
    ** The CasperHttpInfo is a holding object for all of the data parsed out of the HTTP Request and some
    **   other information that is generated from the HTTP parsed headers and URI.
     */
    private final CasperHttpInfo httpInfo;

    /*
    ** The
     */
    private BufferReadMetering metering;

    /*
    ** The readBuffer Operation is the interface to the code that fills in the buffers with data.
    **   In normal execution, this is the NIO handler code that reads from SocketChannel.
     */
    private ReadBuffer readBuffer;

    /*
    **
     */
    private SendFinalStatus sendFinalStatus;

    /*
    **
     */
    private CloseOutRequest closeRequest;

    private DetermineRequestType determineRequestType;

    /*
    ** The metering pointer is used to meter buffers to the read operation. It is used to indicate
    **   where buffers are available to read data into. The read operation has a dependency upon
    **   this pointer.
     */
    private BufferManagerPointer meteringPtr;

    /*
    ** The read pointer is used by the code used to read data into buffers to indicate
    **   where valid data is.
     */
    private BufferManagerPointer readPointer;

    /*
    ** The clientReadDataPtr is where in the data stream consumers are accessing the data. Initially, this is
    **   where the HTTP Parser is accessing buffers.
     */
    private BufferManagerPointer clientDataReadPtr;

    /*
    **
     */
    private BufferManagerPointer clientWritePtr;

    /*
    ** The following is a map of all of the created Operations to handle this request.
     */
    private Map<OperationTypeEnum, Operation> requestHandlerOperations;

    /*
    ** The following map is passed into the RequestContext and it provides a list of all of the Operations that
    **   provide the initial handler for an HTTP Request type. This is setup at the start of execution and
    **   is only populated with handler operations (i.e. V2 PUT).
     */
    private Map<HttpMethodEnum, Operation> supportedHttpRequests;

    /*
    ** The following Map is used to keep track of when the HTTP Request is sent to the
    **   Storage Server for the Web Server and it is used by the test code to know that
    **   the HTTP Request has been sent by the client
    ** The map is based upon the port of the target.
     */
    private Map<Integer, AtomicBoolean> httpRequestSent;

    /*
    ** The following Map is used to indicate that a Storage Server has responded.
     */
    private Map<Integer, Integer> storageServerResponse;

    /*
    ** The httpParseError is set if there is something the parser does not like or a header
    **   does not match the expected values.
     */
    private boolean httpParseError;

    /*
    ** The requestContentLength is how many bytes are going to be transferred following the
    **   HTTP request
     */
    private int requestContentLength;

    /*
    ** This is set when there is either a parsing error or the entire HTTP request has been
    **   parsed and the HTTP Parser has issued the parsing done callback
     */
    private boolean httpRequestParsed;

    /*
     ** Mutex to protect the addition and removal from the work and timed queues
     */
    private final ReentrantLock queueMutex;
    private final Condition queueSignal;
    private boolean workQueued;

    private BlockingQueue<Operation> workQueue;
    private BlockingQueue<Operation> timedWaitQueue;


    public RequestContext(final WebServerFlavor flavor, final MemoryManager memoryManager,
                          final EventPollThread threadThisRunsOn) {

        this.webServerFlavor = flavor;
        this.memoryManager = memoryManager;
        this.threadThisContextRunsOn = threadThisRunsOn;

        /*
         ** Setup the chunk size to use. It is dependent upon if this is running in production or simulation
         */
        chunkSize = TEST_CHUNK_SIZE_IN_BYTES;

        httpInfo = new CasperHttpInfo(this);

        this.clientReadBufferManager = new BufferManager(BUFFER_MGR_RING_SIZE);
        this.clientWriteBufferManager = new BufferManager(BUFFER_MGR_RING_SIZE);
        this.storageServerWriteBufferManager = new BufferManager(BUFFER_MGR_RING_SIZE);
        this.storageServerResponseBufferManager = new BufferManager(STORAGE_SERVER_RESPONSE_RING_SIZE);

        /*
        ** Build the list of all supported HTTP Request and the handler
         */
        this.supportedHttpRequests = new HashMap<>();

        /*
        ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        this.requestHandlerOperations = new HashMap<> ();

        this.storageServerResponse = new HashMap<>();

        /*
        ** Setup the map for the HTTP Request Sent
         */
        this.httpRequestSent = new HashMap<>();

        queueMutex = new ReentrantLock();
        queueSignal = queueMutex.newCondition();
        workQueued = false;

        workQueue = new LinkedBlockingQueue<>(20);
        timedWaitQueue = new LinkedBlockingQueue<>(20);

    }

    /*
    ** This sets up the RequestContext to handle server requests. When it is operating as a
    **   server, the first thing is expects is an HTTP request to arrive. This should be in
    **   the first one or so buffer read in from the connection.
    **
    ** The reading and parsing of the HTTP request is handled by the following operations:
    **   -> BufferReadMetering - This hands out pre-allocated ByteBuffers to the ClientReadBufferManager.
    **   -> ReadBuffer - This informs the IoInterface that there are ByteBuffers ready to have data read into them.
    **          ReadBuffer has a BufferManagerPointer dependency on buffers that are made available by the
    **          BufferReadMetering operation.
    **   -> ParseHttpRequest - This has a dependency on the ClientReadBufferManager and the ReadBuffer BufferManagerPointer.
    **          When the data is read into the ByteBuffer by the IoInterface, it will call the ParseHttpRequest event()
    **          method. This will queue up the operation to be handled by the EventPollThread. When the execute()
    **          method for ParseHttpRequest is called, it will parse all available buffers until it receives the all
    **          headers parsed callback from the HTTP Parser. Once all of the headers are parsed, the DetermineRequestType
    **          operations event() method is called.
    **          The final step for the ParseHttpRequest is to cleanup so that the RequestContext can be used again
    **          to parse another HTTP Request. This will allow a pool of RequestContext to be allocated at start of
    **          day and then reused.
    **
    ** The DetermineRequestType uses the information that the HTTP Parser generated and stored in the CasperHttpInfo
    **   object to setup the correct method handler. There is a list of supported HTTP Methods kept in the
    **   Map<Operation> supportedHttpRequests. Once the correct request is determined, the Operation to setup the
    **   method handler is run.
     */
    public void initializeServer(final IoInterface connection, final int requestId) {
        clientConnection = connection;
        connectionRequestId = requestId;

        /*
         ** Setup the Metering and Read pointers since they are required for the HTTP Parser and for most
         **   HTTP Request handlers.
         */
        metering = new BufferReadMetering(webServerFlavor, this, memoryManager);
        requestHandlerOperations.put(metering.getOperationType(), metering);
        meteringPtr = metering.initialize();

        readBuffer = new ReadBuffer(this, meteringPtr, clientConnection);
        requestHandlerOperations.put(readBuffer.getOperationType(), readBuffer);
        readPointer = readBuffer.initialize();

        /*
         ** The SendFinalStatus and CloseOutRequest are tied together. The SendFinalStatus is
         **   responsible for writing the final status response to the client. The CloseOutRequest
         **   is executed after the write to the client completes and it is responsible for
         **   cleaning up the Request and its associated connection.
         **   Once the cleanup is performed, then the RequestContext is added back to the free list so
         **   it can be used to handle a new request.
         */
        sendFinalStatus = new SendFinalStatus(this, memoryManager, clientConnection);
        requestHandlerOperations.put(sendFinalStatus.getOperationType(), sendFinalStatus);
        clientWritePtr = sendFinalStatus.initialize();

        closeRequest = new CloseOutRequest(this);
        requestHandlerOperations.put(closeRequest.getOperationType(), closeRequest);
        closeRequest.initialize();

        WriteToClient writeToClient = new WriteToClient(this, clientConnection, memoryManager,
                closeRequest, clientWritePtr);
        requestHandlerOperations.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        determineRequestType = new DetermineRequestType(this, supportedHttpRequests);
        requestHandlerOperations.put(determineRequestType.getOperationType(), determineRequestType);
        determineRequestType.initialize();

        SetupV2Put v2PutHandler = new SetupV2Put(this, memoryManager, metering);
        this.supportedHttpRequests.put(HttpMethodEnum.PUT_METHOD, v2PutHandler);

        SetupStorageServerPut storageServerPutHandler = new SetupStorageServerPut(this, memoryManager, metering,
                determineRequestType);
        this.supportedHttpRequests.put(HttpMethodEnum.PUT_STORAGE_SERVER, storageServerPutHandler);

        /*
         ** Setup the specific part for parsing the buffers as an HTTP Request.
         */
        initializeHttpParsing();
    }

    /*
    ** This is used to setup the common HTTP Request parsing Operations and their dependencies
     */
    void initializeHttpParsing() {
        /*
         **
         */
        httpParseError = false;
        httpRequestParsed = false;
        requestContentLength = 0;

        /*
         ** The two Operations that run to perform the HTTP Parsing are ParseHttpRequest and
         **   DetermineRequestType. When the entire HTTP Request has been parsed, the ParseHttpRequest
         **   will event the DetermineRequestType operation to determine what operation sequence
         **   to setup.
         */
        ParseHttpRequest httpParser = new ParseHttpRequest(this, readPointer, metering, determineRequestType);
        requestHandlerOperations.put(httpParser.getOperationType(), httpParser);
        clientDataReadPtr = httpParser.initialize();

        /*
        ** Now Meter out a buffer to read in the HTTP request
         */
        metering.event();
    }

    /*
    ** This is used to cleanup the HTTP Request parsing Operations and their dependencies
     */
    public void cleanupHttpParser() {
        Operation operation;

        operation = requestHandlerOperations.get(OperationTypeEnum.PARSE_HTTP_BUFFER);
        operation.complete();
        requestHandlerOperations.remove(OperationTypeEnum.PARSE_HTTP_BUFFER);
    }

    /*
    ** This is called when the request has completed and this RequestContext needs to be put back into
    **   a pristine state so it can be used for a new request.
    ** Once it is cleaned up, this RequestContext is added back to the free list so it can be used again.
     */
    public void cleanupServerRequest() {

        clientConnection.closeConnection();

        Operation operation;

        operation = requestHandlerOperations.get(OperationTypeEnum.WRITE_TO_CLIENT);
        operation.complete();

        operation = requestHandlerOperations.get(OperationTypeEnum.SEND_FINAL_STATUS);
        operation.complete();

        operation = requestHandlerOperations.get(OperationTypeEnum.REQUEST_FINISHED);
        operation.complete();

        operation = requestHandlerOperations.get(OperationTypeEnum.READ_BUFFER);
        operation.complete();

        operation = requestHandlerOperations.get(OperationTypeEnum.METER_READ_BUFFERS);
        operation.complete();

        /*
        ** Now reset the BufferManagers back to their pristine state. That means that there are no
        **   registered BufferManagerPointers or dependencies remaining associated with the
        **   BufferManager.
         */
        clientReadBufferManager.reset();
        clientWriteBufferManager.reset();
        storageServerWriteBufferManager.reset();
        storageServerResponseBufferManager.reset();

        /*
        ** Finally release the clientConnection back to the free pool.
         */
        releaseConnection(clientConnection);
        clientConnection = null;
    }

    /*
    **
     */
    public void reset() {

        supportedHttpRequests.clear();
        storageServerResponse.clear();
        httpRequestSent.clear();

        dumpOperations();
        requestHandlerOperations.clear();
    }

    /*
    ** This is the method that is called to execute all of the Operations that have been
    **   placed in the "ready to run" state as the result of their "event" being triggered.
    ** This is the method that is called by the Event Thread responsible for this request.
     */
    public void performOperationWork() {
        List<Operation> operationsToRun = new ArrayList<>();

        try {
            queueMutex.lock();
            try {
                if (workQueued || queueSignal.await(100, TimeUnit.MILLISECONDS)) {
                    int drainedCount = workQueue.drainTo(operationsToRun, MAX_EXEC_WORK_LOOP_COUNT);
                    for (Operation operation : operationsToRun) {
                        operation.markRemovedFromQueue(false);
                    }

                    /*
                     ** Make sure this is only modified with the queueMutex lock. If there might still be
                     **   elements on the queue, do not clear the workQueued flag.
                     */
                    if (drainedCount != MAX_EXEC_WORK_LOOP_COUNT) {
                        workQueued = false;
                    }
                }
            } finally {
                queueMutex.unlock();
            }

            for (Operation operation : operationsToRun) {
                LOG.info("requestId[" + connectionRequestId + "] operation(" + operation.getOperationType() + ") execute");
                operation.execute();
            }

            /*
             ** Check if there are ConnectionState that are on the timedWaitQueue and their
             **   wait time has elapsed. Currently, this is an ordered queue and everything
             **   on the queue has the same wait time so only the head element needs to be
             **   checked for the elapsed timeout.
             */
            Operation operation;
            do {
                queueMutex.lock();
                try {
                    if ((operation = timedWaitQueue.peek()) != null) {
                        if (operation.hasWaitTimeElapsed()) {
                            timedWaitQueue.remove(operation);
                            operation.markRemovedFromQueue(true);
                        } else {
                            operation = null;
                        }
                    }
                } finally {
                    queueMutex.unlock();
                }

                if (operation != null) {
                    LOG.info("requestId[" + connectionRequestId + "] operation(" + operation.getOperationType() + ") timed execute");

                    operation.execute();
                }
            } while (operation != null);
        } catch (InterruptedException int_ex) {
            /*
            ** Need to close out this request since something serious has gone wrong.
             */
        }
    }

    /*
     ** This checks to see if there is space in the queue and then adds the
     ** ConnectionState object if there is. It returns false if there is no
     ** space currently in the queue.
     */
    public void addToWorkQueue(final Operation operation) {
        queueMutex.lock();
        try {
            LOG.info("requestId[" + connectionRequestId + "] addToWorkQueue() operation(" + operation.getOperationType() + ") onExecutionQueue: " +
                     operation.isOnWorkQueue() + " onTimedWaitQueue: " + operation.isOnTimedWaitQueue());

            if (!operation.isOnWorkQueue()) {
                if (operation.isOnTimedWaitQueue()) {
                    if (timedWaitQueue.remove(operation)) {
                        operation.markRemovedFromQueue(true);
                    } else {
                        LOG.error("requestId[" + connectionRequestId + "] addToWorkQueue() not on timedWaitQueue");
                    }
                }

                if (!workQueue.offer(operation)) {
                    LOG.error("requestId[" + connectionRequestId + "] addToWorkQueue() unable to add");
                } else {
                    operation.markAddedToQueue(false);
                    queueSignal.signal();

                    workQueued = true;
                }
            }
        } finally {
            queueMutex.unlock();
        }
    }

    public void addToDelayedQueue(final Operation operation) {
        queueMutex.lock();
        try {
            //LOG.info("requestId[" + connectionRequestId + "] addToDelayedQueue() onExecutionQueue: " + operation.isOnWorkQueue() +
            //        " onTimedWaitQueue: " + operation.isOnTimedWaitQueue());

            if (!operation.isOnWorkQueue() && !operation.isOnTimedWaitQueue()) {
                if (!timedWaitQueue.offer(operation)) {
                    LOG.error("requestId[" + connectionRequestId + "] addToWorkQueue() unable to add");
                } else {
                    operation.markAddedToQueue(true);
                }
            }
        } finally {
            queueMutex.unlock();
        }
    }

    /*
     ** This removes the connection from whichever queue it is on. It emits a debug statement if it is not on
     **   the expected queue.
     */
    public void removeFromQueue(final Operation operation) {
        queueMutex.lock();
        try {
            if (workQueue.remove(operation)) {
                //LOG.info("requestId[" + connectionRequestId + "] removeFromQueue() workQueue");
                operation.markRemovedFromQueue(false);
            } else if (timedWaitQueue.remove(operation)) {
                //LOG.info("requestId[" + connectionRequestId + "] removeFromQueue() timeWaitQueue");
                operation.markRemovedFromQueue(true);
            } else {
                LOG.warn("requestId[" + connectionRequestId + "] removeFromQueue() not on any queue");
            }
        } finally {
            queueMutex.unlock();
        }
    }

    /*
    ** This is a test function to validate a certain Operation is on the execute queue.
    **
    ** NOTE: This uses iterator() so that the contents of the workQueue or not modified.
     */
    public boolean validateOperationOnQueue(final OperationTypeEnum operationType) {
        boolean found = false;
        int operationsCount = 0;

        queueMutex.lock();
        try {
            Iterator<Operation> operationsToRun = workQueue.iterator();

            while (operationsToRun.hasNext()) {
                if (operationsToRun.next().getOperationType() == operationType) {
                    found = true;
                    break;
                }
                operationsCount++;
            }
        } finally {
            queueMutex.unlock();
        }

        if (!found) {
            LOG.warn("requestId[" + connectionRequestId + "] Operation(" + operationType + ") not found. drainedCount: " +
                    operationsCount);
        }

        return found;
    }

    /*
    ** Allocation and free routines used for connections (IoInterface). The connections are tied to a specific
    **   NioEventPollThread.
     */
    public IoInterface allocateConnection(final Operation requestingOperation) {
        return threadThisContextRunsOn.allocateConnection(requestingOperation);
    }

    public void releaseConnection(final IoInterface connection) {
        threadThisContextRunsOn.releaseConnection(connection);
    }

    /*
    ** Used to obtain the Chunk size used
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /*
    ** Used to allow tracing based upon a connection identifier
     */
    public int getRequestId() {
        return connectionRequestId;
    }

    public CasperHttpInfo getHttpInfo() {
        return httpInfo;
    }

    public BufferManager getClientReadBufferManager() {
        return clientReadBufferManager;
    }

    public BufferManager getClientWriteBufferManager() {
        return clientWriteBufferManager;
    }

    public BufferManager getStorageServerWriteBufferManager() {
        return storageServerWriteBufferManager;
    }

    public void setHttpParsingError() {
        httpParseError = true;
        httpRequestParsed = true;
    }

    public int getHttpParseStatus() {
        int parsingStatus = HttpStatus.OK_200;
        if (httpParseError) {
            parsingStatus = httpInfo.getParseFailureCode();
        }

        return parsingStatus;
    }


    public void httpHeaderParseComplete(final int contentLength) {
        LOG.info("requestId[" + connectionRequestId + "] httpHeaderParseComplete() contentLength: " + contentLength);

        requestContentLength = contentLength;
        httpRequestParsed = true;
    }

    public boolean isHttpRequestParsed() {
        return httpRequestParsed;
    }

    public boolean getHttpParseError() {
        return httpParseError;
    }

    public int getRequestContentLength() {
        return requestContentLength;
    }

    /*
    ** The following are getters for test specific code.
     */
    public BufferManagerPointer getReadBufferPointer() {
        return readPointer;
    }

    /*
    **
     */
    public boolean hasHttpRequestBeenSent(final int targetPort) {
        AtomicBoolean responseSent = httpRequestSent.get(targetPort);
        if (responseSent != null) {
            if (responseSent.get()) {
                httpRequestSent.remove(targetPort);
                return true;
            }
        }
        return false;
    }

    public void setHttpResponseSent(final int targetPort) {
        AtomicBoolean httpSent = new AtomicBoolean(true);
        httpRequestSent.put(targetPort, httpSent);
    }

    public boolean hasStorageServerResponseArrived(final int targetPort) {
        Integer responseSent = storageServerResponse.get(targetPort);
        if (responseSent != null) {
            return true;
        }
        return false;
    }

    public int getStorageResponseResult(final int targetPort) {
        Integer responseSent = storageServerResponse.get(targetPort);
        if (responseSent != null) {
            return responseSent;
        }
        return -1;
    }

    public void setStorageServerResponse(final int targetPort, final int result) {
        Integer storageServerResult = new Integer(result);
        storageServerResponse.put(targetPort, storageServerResult);
    }

    /*
    **
     */
    public Operation getOperation(final OperationTypeEnum operationType) {
        return requestHandlerOperations.get(operationType);
    }

    public void addOperation(final Operation operation) {
        requestHandlerOperations.put(operation.getOperationType(), operation);
    }

    public void dumpOperations() {
        LOG.info(" RequestContext[" + connectionRequestId + "] Operation dependency");
        Collection<Operation> createdOperations = requestHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(1);
        }

    }
}
