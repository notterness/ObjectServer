package com.webutils.webserver.requestcontext;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/*
** The RequestContext is how the lifecycle of an HTTP Request is handled. It contains the various state and
**   generated information for the request.
 */
public abstract class RequestContext {

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


    private static final int MAX_EXEC_WORK_LOOP_COUNT = 10;

    private final WebServerFlavor webServerFlavor = WebServerFlavor.INTEGRATION_TESTS;

    /*
    ** This is the Chunk Size used
     */
    private final int chunkSize;

    /*
     ** The HttpXferInfo is a holding object for all of the data parsed out of the HTTP Request and some
     **   other information that is generated from the HTTP parsed headers and URI.
     */
    private final HttpRequestInfo httpInfo;

    /*
     ** This is the BufferManager to read data in from the client.
     **   The filled buffers may be the from the NIO layer, in which case the buffers are being filled through read
     **     calls on the SocketChannel.
     **   Or for one of the debug cases, the buffers are filled from a file read.
     */
    protected final BufferManager clientReadBufferManager;

    /*
     ** This is the BufferManager used to write data out to the client.
     */
    protected final BufferManager clientWriteBufferManager;

    /*
     ** The readBuffer Operation is the interface to the code that fills in the buffers with data.
     **   In normal execution, this is the NIO handler code that reads from SocketChannel.
     */
    protected ReadBuffer readBuffer;

    /*
     ** The read pointer is used by the code used to read data into buffers to indicate
     **   where valid data is.
     */
    protected BufferManagerPointer readPointer;

    /*
     ** The BufferReadMeter is used to allow buffers to be accessed by the input source. It provides a mechanism to
     **   limit the rate of client requests.
     */
    protected BufferReadMetering metering;

    /*
    ** This is used to know which thread made the request to allocate the RequestContext
     */
    private final int threadId;

    /*
    ** A connection may have multiple requests within it. For example:
    **    GET, GET, GET
     */
    protected int connectionRequestId;

    /*
    ** The IoInterface is used to read and write data from the client side. If this is in a production
    **   setup (and many test setups) the client side will be obtained through an NIO ServerSocketChannel
    **   accept() call and then the reads and writes will occur on an NIO SocketChannel.
    **
    ** For certain test cases, the IoInterface allows a file to be used to read data in from as well.
     */
    protected IoInterface clientConnection;

    /*
     ** The memory manager used to populate the various BufferManagers
     */
    protected final MemoryManager memoryManager;

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
    ** The following is a map of all of the created Operations to handle this request.
     */
    protected Map<OperationTypeEnum, Operation> requestHandlerOperations;

    /*
    ** The following map is passed into the RequestContext and it provides a list of all of the Operations that
    **   provide the initial handler for an HTTP Request type. This is setup at the start of execution and
    **   is only populated with handler operations (i.e. V2 PUT).
     */
    protected Map<HttpMethodEnum, Operation> supportedHttpRequests;

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

    /*
     ** The next two variables are used to keep track of the Sha-256 Digest calculation. First if it has been
     **   completed and second if the calculated Sha-256 digest matches the expected one.
     */
    private boolean sha256DigestComplete;
    private boolean contentHasValidSha256Digest;

    private boolean v2PutAllDataWritten;
    private boolean postMethodContentDataParsed;

    /*
     ** Mutex to protect the addition and removal from the work and timed queues
     */
    private final ReentrantLock queueMutex;
    private final Condition queueSignal;
    private boolean workQueued;

    private final BlockingQueue<Operation> workQueue;
    private final BlockingQueue<Operation> timedWaitQueue;


    public RequestContext(final MemoryManager memoryManager, final EventPollThread threadThisRunsOn, final DbSetup dbSetup,
                          final int threadId) {

        this.memoryManager = memoryManager;
        this.threadThisContextRunsOn = threadThisRunsOn;
        this.dbSetup = dbSetup;
        this.threadId = threadId;

        /*
         ** Setup the chunk size to use. It is dependent upon if this is running in production or simulation
         */
        chunkSize = TEST_CHUNK_SIZE_IN_BYTES;

        /*
         ** The BufferManager(s) that are allocated here are populated in the following Operations:
         **   clientReadBufferManager - This is populated with ByteBuffer(s) in the BufferReadMetering operation
         **   clientWriteBufferManager - This is populated in the SendFinalStatus operation, but will need to be changed
         **     once the GET request is implemented to allowing streaming of data back to the clients.
         */
        int bufferMgrRingSize = memoryManager.getBufferManagerRingSize();
        this.clientReadBufferManager = new BufferManager(bufferMgrRingSize, "ClientRead", 100);
        this.clientWriteBufferManager = new BufferManager(bufferMgrRingSize, "ClientWrite", 200);

        httpInfo = new HttpRequestInfo(this);

        /*
        ** Build the list of all supported HTTP Request and the handler
         */
        this.supportedHttpRequests = new HashMap<>();

        /*
        ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        this.requestHandlerOperations = new HashMap<> ();

        queueMutex = new ReentrantLock();
        queueSignal = queueMutex.newCondition();
        workQueued = false;

        workQueue = new LinkedBlockingQueue<>(20);
        timedWaitQueue = new LinkedBlockingQueue<>(20);

        digestComplete = false;
        contentHasValidMd5Digest = false;

        v2PutAllDataWritten = false;
        postMethodContentDataParsed = false;
    }

    /*
    ** The following need to have at least stubs in all of the sub-classes. There is probably a better way to
    **   handle the problem without having to use an interface class for the RequestContext resource pool.
     */
    public abstract void initializeServer(final IoInterface connection, final int requestId);

    public abstract void cleanupHttpParser();


    public abstract boolean hasHttpRequestBeenSent(final ServerIdentifier storageServerId);
    public abstract void setHttpRequestSent(final ServerIdentifier storageServerId);
    public abstract void removeHttpRequestSent(final ServerIdentifier storageServerId);

    public abstract boolean hasStorageServerResponseArrived(final ServerIdentifier storageServerId);
    public abstract int getStorageResponseResult(final ServerIdentifier storageServerId);
    public abstract void setStorageServerResponse(final ServerIdentifier storageServerId, final int result);
    public abstract void removeStorageServerResponse(final ServerIdentifier storageServerId);
    public abstract BufferManager getStorageServerWriteBufferManager();

    /*
    ** This is called when the request has completed and this RequestContext needs to be put back into
    **   a pristine state so it can be used for a new request.
    ** Once it is cleaned up, this RequestContext is added back to the free list so it can be used again.
     */
    public void cleanupServerRequest() {

        clientConnection.closeConnection();

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

        supportedHttpRequests.clear();

        /*
        ** The dumpOperations() should have either the SETUP_V2_PUT or SETUP_STORAGE_SERVER_PUT in its
        **   list.
         */
        dumpOperations();
        requestHandlerOperations.clear();

        digestComplete = false;
        contentHasValidMd5Digest = false;
        sha256DigestComplete = false;
        contentHasValidSha256Digest = false;
        v2PutAllDataWritten = false;
        postMethodContentDataParsed = false;
    }

    /*
    ** Accessor function to obtain the threadId. This is used in the RequstContext pool allocation classes so that the
    **   threadId does not need to be passwd into the releaseContext() method.
     */
    public int getThreadId() {
        return threadId;
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

    protected void initializeHttpParsing() {
        /*
         **
         */
        httpParseError = false;
        httpRequestParsed = false;
        requestContentLength = 0;
    }

    /*
    ** The getHttpInfo() getter is used to access the CasperHttpInfo where the details about the HTTP Request are
    **   kept.
     */
    public HttpRequestInfo getHttpInfo() {
        return httpInfo;
    }

    public BufferManager getClientReadBufferManager() {
        return clientReadBufferManager;
    }

    public BufferManager getClientWriteBufferManager() {
        return clientWriteBufferManager;
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
     ** Accessor methods for the Md5 Digest information
     */
    public void setDigestComplete() {
        digestComplete = true;
    }

    public void setMd5DigestCompareResult(final boolean valid) {
        if (!valid) {
            /*
            ** Set the error response (This should probably be more descriptive to indicate that the Md5 digest failed)
             */
            httpInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400);
        }

        contentHasValidMd5Digest = valid;
    }

    public boolean getDigestComplete() {
        return digestComplete;
    }

    public boolean getMd5DigestResult() {
        return contentHasValidMd5Digest;
    }

    /*
     ** Accessor methods for the Sha-256 Digest information
     */
    public void setSha256DigestComplete() {
        sha256DigestComplete = true;
    }

    public void setSha256DigestCompareResult(final boolean valid) {
        if (!valid) {
            /*
             ** Set the error response (This should probably be more descriptive to indicate that the Sha-256 digest failed)
             */
            httpInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400);
        }

        contentHasValidSha256Digest = valid;
    }

    public boolean getSha256DigestComplete() {
        return sha256DigestComplete;
    }

    public boolean getSha256DigestResult() {
        return contentHasValidSha256Digest;
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
    **
     */
    public void setPostMethodContentParsed() { postMethodContentDataParsed = true; }
    public boolean postMethodContentParsed() { return postMethodContentDataParsed; }

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

    public WebServerFlavor getWebServerFlavor() {
        return webServerFlavor;
    }

    public void dumpOperations() {
        LOG.info(" RequestContext[" + connectionRequestId + "] Operation dependency");
        Collection<Operation> createdOperations = requestHandlerOperations.values();
        for (Operation createdOperation : createdOperations) {
            createdOperation.dumpCreatedOperations(1);
        }

    }
}
