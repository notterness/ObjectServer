package com.webutils.webserver.requestcontext;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.CasperHttpInfo;
import com.webutils.webserver.http.HttpMethodEnum;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.operations.*;
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


    public static final int TEST_BUFFER_MGR_RING_SIZE = 32;
    public static final int PRODUCTION_BUFFER_MGR_RING_SIZE = 4096;

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
    ** Used to gain access to the MySql database
     */
    private final DbSetup dbSetup;

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
    ** The read pointer is used by the code used to read data into buffers to indicate
    **   where valid data is.
     */
    private BufferManagerPointer readPointer;

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
    **   Storage Server from the Web Server and it is used by the test code to know that
    **   the HTTP Request has been sent by the client to the Web Server.
    ** The map is based upon the IP address and the TCP Port of the target plus the chunk number.
     */
    private Map<ServerIdentifier, AtomicBoolean> httpRequestSent;

    /*
    ** The following Map is used to indicate that a Storage Server has responded.
     */
    private Map<ServerIdentifier, Integer> storageServerResponse;

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
    ** The next two variables are used to keep track of the Md5 Digest calculation. First if it has been
    **   completed and second if the calculated Md5 digest matches the expected one.
     */
    private boolean digestComplete;
    private boolean contentHasValidMd5Digest;

    private boolean v2PutAllDataWritten;

    /*
     ** Mutex to protect the addition and removal from the work and timed queues
     */
    private final ReentrantLock queueMutex;
    private final Condition queueSignal;
    private boolean workQueued;

    private BlockingQueue<Operation> workQueue;
    private BlockingQueue<Operation> timedWaitQueue;


    public RequestContext(final WebServerFlavor flavor, final MemoryManager memoryManager,
                          final EventPollThread threadThisRunsOn, final DbSetup dbSetup) {

        this.webServerFlavor = flavor;
        this.memoryManager = memoryManager;
        this.threadThisContextRunsOn = threadThisRunsOn;
        this.dbSetup = dbSetup;

        /*
         ** Setup the chunk size to use. It is dependent upon if this is running in production or simulation
         */
        chunkSize = TEST_CHUNK_SIZE_IN_BYTES;

        httpInfo = new CasperHttpInfo(this);

        /*
        ** The BufferManager(s) that are allocated here are populated in the following Operations:
        **   clientReadBufferManager - This is populated with ByteBuffer(s) in the BufferReadMetering operation
        **   clientWriteBufferManager - This is populated in the SendFinalStatus operation, but will need to be changed
        **     once the GET request is implemented to allowing streaming of data back to the clients.
        **   storageServerWriteBufferManager - This is populated in the EncryptBuffer operation.
         */
        int bufferMgrRingSize = getBufferManagerRingSize();
        this.clientReadBufferManager = new BufferManager(bufferMgrRingSize, "ClientRead", 100);
        this.clientWriteBufferManager = new BufferManager(bufferMgrRingSize, "ClientWrite", 200);
        this.storageServerWriteBufferManager = new BufferManager(bufferMgrRingSize, "StorageServerWrite", 300);

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

        digestComplete = false;
        contentHasValidMd5Digest = false;

        v2PutAllDataWritten = false;
    }

    /*
    ** This sets up the RequestContext to handle server requests. When it is operating as a
    **   server, the first thing is expects is an HTTP request to arrive. This should be in
    **   the first one or two buffers read in from the connection.
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
        metering = new BufferReadMetering(this, memoryManager);
        requestHandlerOperations.put(metering.getOperationType(), metering);
        BufferManagerPointer meteringPtr = metering.initialize();

        readBuffer = new ReadBuffer(this, meteringPtr, clientConnection);
        requestHandlerOperations.put(readBuffer.getOperationType(), readBuffer);
        readPointer = readBuffer.initialize();

        /*
         ** The SendFinalStatus, WriteToClient and CloseOutRequest are tied together. The SendFinalStatus is
         **   responsible for building the final HTTP status response to the client. The WriteToClient is
         **   responsible for kicking the IoInterface to write the data out and waiting for the data
         **   pointer to be updated to know that the data has been written. Once the data has been all written, then
         **   the WriteToClient operation will event() the CloseOutRequest operation.
         **   The CloseOutRequest is executed after the write to the client completes and it is responsible for
         **   cleaning up the Request and its associated connection.
         **   Once the cleanup is performed, then the RequestContext is added back to the free list so
         **   it can be used to handle a new request.
         */
        sendFinalStatus = new SendFinalStatus(this, memoryManager, clientConnection);
        requestHandlerOperations.put(sendFinalStatus.getOperationType(), sendFinalStatus);
        BufferManagerPointer clientWritePtr = sendFinalStatus.initialize();

        closeRequest = new CloseOutRequest(this);
        requestHandlerOperations.put(closeRequest.getOperationType(), closeRequest);
        closeRequest.initialize();

        WriteToClient writeToClient = new WriteToClient(this, clientConnection,
                closeRequest, clientWritePtr);
        requestHandlerOperations.put(writeToClient.getOperationType(), writeToClient);
        writeToClient.initialize();

        /*
        ** The DetermineRequestType operation is run after the HTTP Request has been parsed and the method
        **   handler determined via the setHttpMethodAndVersion() method in the CasperHttpInfo object.
         */
        determineRequestType = new DetermineRequestType(this, supportedHttpRequests);
        requestHandlerOperations.put(determineRequestType.getOperationType(), determineRequestType);
        determineRequestType.initialize();

        /*
        ** The HTTP Request methods that are supported are added to the supportedHttpRequests Map<> and are used
        **   by the DetermineRequestType operation to setup and run the appropriate handlers.
         */
        SetupV2Put v2PutHandler = new SetupV2Put(this, memoryManager, metering, determineRequestType);
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
        httpParser.initialize();

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

        operation = requestHandlerOperations.remove(OperationTypeEnum.PARSE_HTTP_BUFFER);
        operation.complete();
    }

    /*
    ** This is called when the request has completed and this RequestContext needs to be put back into
    **   a pristine state so it can be used for a new request.
    ** Once it is cleaned up, this RequestContext is added back to the free list so it can be used again.
     */
    public void cleanupServerRequest() {

        clientConnection.closeConnection();

        Operation operation;

        operation = requestHandlerOperations.remove(OperationTypeEnum.WRITE_TO_CLIENT);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.SEND_FINAL_STATUS);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.REQUEST_FINISHED);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.READ_BUFFER);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.METER_READ_BUFFERS);
        operation.complete();

        operation = requestHandlerOperations.remove(OperationTypeEnum.DETERMINE_REQUEST_TYPE);
        operation.complete();

        /*
        ** Clear out the references to the Operations
         */
        metering = null;
        readBuffer = null;
        sendFinalStatus = null;
        closeRequest = null;
        determineRequestType = null;

        /*
        ** Call reset() to make sure the BufferManager(s) have released all the references to
        **   ByteBuffer(s).
         */
        reset();

        /*
        ** Finally release the clientConnection back to the free pool.
         */
        releaseConnection(clientConnection);
        clientConnection = null;
    }

    /*
    ** This is used to clean up both the Client and Server side for the RequestContext.
     */
    public void reset() {

        /*
         ** Now reset the BufferManagers back to their pristine state. That means that there are no
         **   registered BufferManagerPointers or dependencies remaining associated with the
         **   BufferManager.
         */
        clientReadBufferManager.reset();
        clientWriteBufferManager.reset();
        storageServerWriteBufferManager.reset();

        /*
        ** Clear out the Map<> associated with HTTP Requests and Responses from the Storage Server and then
        **   the Map<> that keeps track if an HTTP Request has been sent (used for the client side).
         */
        supportedHttpRequests.clear();
        storageServerResponse.clear();
        httpRequestSent.clear();

        /*
        ** The dumpOperations() should have either the SETUP_V2_PUT or SETUP_STORAGE_SERVER_PUT in its
        **   list.
         */
        dumpOperations();
        requestHandlerOperations.clear();

        digestComplete = false;
        contentHasValidMd5Digest = false;
        v2PutAllDataWritten = false;
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
             ** Check if there are Operations that are on the timedWaitQueue and their
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
            if (!operation.isOnWorkQueue()) {
                /*
                 ** Only log if it is not on the work queue already
                 */
                LOG.info("requestId[" + connectionRequestId + "] addToWorkQueue() operation(" +
                        operation.getOperationType() + ")");

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
    **
     */
    public void runComputeWork(final Operation operation) {
        if (!threadThisContextRunsOn.runComputeWork(operation)) {
            addToWorkQueue(operation);
        }
    }

    public void removeComputeWork(final Operation operation) {
        threadThisContextRunsOn.removeComputeWork(operation);
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

            for (Operation operation : workQueue) {
                if (operation.getOperationType() == operationType) {
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

    /*
    ** The getHttpInfo() getter is used to access the CasperHttpInfo where the details about the HTTP Request are
    **   kept.
     */
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
    ** The following getter is used to get access to the read BufferManagerPointer that is setup to
    **   read in the HTTP Request initially. For a PUT operation, that same read pointer is used to bring in the
    **   client object data.
     */
    public BufferManagerPointer getReadBufferPointer() {
        return readPointer;
    }

    /*
    ** The following are used to keep track of Storage Servers and if the HTTP Request has been sent successfully
    **   to it. The setter (setHttpResponseSent() is called by WriteHeaderToStorageServer after the buffer has been
    **   written to the SocketChannel.
     */
    public boolean hasHttpRequestBeenSent(final ServerIdentifier storageServerId) {
        AtomicBoolean responseSent = httpRequestSent.get(storageServerId);
        if (responseSent != null) {
            return responseSent.get();
        }
        return false;
    }

    public void setHttpRequestSent(final ServerIdentifier storageServerId) {
        AtomicBoolean httpSent = new AtomicBoolean(true);
        httpRequestSent.put(storageServerId, httpSent);
    }

    /*
    ** This is used to remove the Map<> entry for the Storage Server after it has completed it's writes
     */
    public void removeHttpRequestSent(final ServerIdentifier storageServerId) {
        if (httpRequestSent.remove(storageServerId) == null) {
            LOG.warn("RequestContext[" + getRequestId() + "] HTTP Request remove failed targetPort: " +
                    storageServerId.getStorageServerIpAddress() + ":" + storageServerId.getStorageServerTcpPort() +
                    ":" + storageServerId.getChunkNumber());

        }
    }

    /*
    ** The following are used to keep track of the HTTP Response from the Storage Server.
     */
    public boolean hasStorageServerResponseArrived(final ServerIdentifier storageServerId) {
        Integer responseSent = storageServerResponse.get(storageServerId);
        return (responseSent != null);
    }

    public int getStorageResponseResult(final ServerIdentifier storageServerId) {
        Integer responseSent = storageServerResponse.get(storageServerId);
        if (responseSent != null) {
            return responseSent;
        }
        return -1;
    }

    public void setStorageServerResponse(final ServerIdentifier storageServerId, final int result) {
        Integer storageServerResult = new Integer(result);
        storageServerResponse.put(storageServerId, storageServerResult);
    }

    public void removeStorageServerResponse(final ServerIdentifier storageServerId) {
        if (storageServerResponse.remove(storageServerId) == null) {
            LOG.warn("RequestContext[" + getRequestId() + "] HTTP Response remove failed targetPort: " +
                    storageServerId.getStorageServerIpAddress() + ":" + storageServerId.getStorageServerTcpPort() +
                    ":" + storageServerId.getChunkNumber());
        }
    }

    /*
    ** Accessor methods for the Md5 Digest information
     */
    public void setDigestComplete() {
        digestComplete = true;
    }

    public void setMd5DigestCompareResult(final boolean valid) {
        contentHasValidMd5Digest = valid;
    }

    public boolean getDigestComplete() {
        return digestComplete;
    }

    public boolean getMd5DigestResult() {
        return contentHasValidMd5Digest;
    }

    /*
    ** Acccessor methods to keep track of when all the data has been written to the Storage Server(s)
     */
    public void setAllV2PutDataWritten() {
        v2PutAllDataWritten = true;
    }

    public boolean getAllV2PutDataWritten() {
        return v2PutAllDataWritten;
    }

    /*
    ** Value for the number of ByteBuffer(s) in the BufferManager rings
     */
    public int getBufferManagerRingSize() {
        if ((webServerFlavor == WebServerFlavor.DOCKER_OBJECT_SERVER_PRODUCTION) ||
                (webServerFlavor == WebServerFlavor.DOCKER_STORAGE_SERVER_PRODUCTION)) {
            return PRODUCTION_BUFFER_MGR_RING_SIZE;
        } else {
            return TEST_BUFFER_MGR_RING_SIZE;
        }
    }

    /*
    ** Used to obtain the DbSetup object which is used to access the Storage Server information in the MySql database
     */
    public DbSetup getDbSetup() {
        return dbSetup;
    }

    /*
    ** The following methods are used to keep track of the Operations that have been started by the RequestContext and
    **   which ones are currently running.
     */
    public Operation getOperation(final OperationTypeEnum operationType) {
        return requestHandlerOperations.get(operationType);
    }

    public void addOperation(final Operation operation) {
        requestHandlerOperations.put(operation.getOperationType(), operation);
    }

    public String getIoInterfaceIdentifier() {
        return clientConnection.getIdentifierInfo();
    }

    public void dumpOperations() {
        LOG.info(" RequestContext[" + connectionRequestId + "] Operation dependency");
        Collection<Operation> createdOperations = requestHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(1);
        }

    }
}
