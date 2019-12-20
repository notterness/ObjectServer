package com.oracle.athena.webserver.requestcontext;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.connectionstate.CasperHttpInfo;
import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.HttpMethodEnum;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.operations.*;
import com.oracle.athena.webserver.server.ServerLoadBalancer;
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
public class RequestContext {

    private static final Logger LOG = LoggerFactory.getLogger(RequestContext.class);


    private static final int BUFFER_MGR_RING_SIZE = 128;
    private static final int STORAGE_SERVER_RESPONSE_RING_SIZE = 10;

    private static final int MAX_EXEC_WORK_LOOP_COUNT = 10;

    /*
    ** A connection may have multiple requests within it. For example:
    **    GET, GET, GET
     */
    private final int connectionRequestId;

    /*
    ** The memory manager used to populate the various BufferManagers
     */
    private final MemoryManager memoryManager;

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
    private final BufferReadMetering metering;

    /*
    ** The readBuffer Operation is the interface to the code that fills in the buffers with data.
    **   In normal execution, this is the NIO handler code that reads from SocketChannel.
     */
    private final ReadBuffer readBuffer;

    /*
    **
     */
    private final SendFinalStatus sendFinalStatus;

    /*
    **
     */
    private final CloseOutRequest closeRequest;

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
    private BufferManagerPointer readPtr;

    /*
    **
     */
    private BufferManagerPointer clientWritePtr;

    private BufferManagerPointer clientWriteDonePtr;


    /*
    ** The following is a map of all of the created Operations to handle this request.
     */
    private Map<OperationTypeEnum, Operation> requestHandlerOperations;

    /*
    ** The following map is passed into the RequestContext and it provides a list of all of the Operations that
    **   provide the initial handler for an HTTP Request type. This is setup at the start of execution and
    **   is only populated with handler operations (i.e. V2 PUT).
     */
    private final Map<HttpMethodEnum, Operation> supportedHttpRequests;

    /*
    ** The httpParseError is set if there is something the parser does not like or a header
    **   does not match the expected values.
     */
    private boolean httpParseError;

    /*
    ** The requestContentLength is how many bytes are going to be transferred following the
    **   HTTP request
     */
    private long requestContentLength;

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


    public RequestContext(final int requestId, final MemoryManager memoryManager) {

        this.connectionRequestId = requestId;
        this.memoryManager = memoryManager;

        httpInfo = new CasperHttpInfo(this);

        this.clientReadBufferManager = new BufferManager(BUFFER_MGR_RING_SIZE);
        this.clientWriteBufferManager = new BufferManager(BUFFER_MGR_RING_SIZE);
        this.storageServerWriteBufferManager = new BufferManager(BUFFER_MGR_RING_SIZE);
        this.storageServerResponseBufferManager = new BufferManager(STORAGE_SERVER_RESPONSE_RING_SIZE);

        /*
        ** Build the list of all supported HTTP Request and the handler
         */
        this.supportedHttpRequests = new HashMap<HttpMethodEnum, Operation>();

        SetupV2Put v2PutHandler = new SetupV2Put(this);
        this.supportedHttpRequests.put(HttpMethodEnum.PUT_METHOD, v2PutHandler);

        /*
        ** Setup this RequestContext to be able to read in and parse the HTTP Request(s)
         */
        requestHandlerOperations = new HashMap<OperationTypeEnum, Operation> ();

        queueMutex = new ReentrantLock();
        queueSignal = queueMutex.newCondition();
        workQueued = false;

        workQueue = new LinkedBlockingQueue<>(20);
        timedWaitQueue = new LinkedBlockingQueue<>(20);

        /*
         ** Setup the Metering and Read pointers since they are required for the HTTP Parser and for most
         **   HTTP Request handlers.
         */
        metering = new BufferReadMetering(this);
        requestHandlerOperations.put(metering.getOperationType(), metering);
        meteringPtr = metering.initialize();

        readBuffer = new ReadBuffer(this, meteringPtr);
        requestHandlerOperations.put(readBuffer.getOperationType(), readBuffer);
        readPtr = readBuffer.initialize();

        /*
        ** The SendFinalStatus and CloseOutRequest are tied together. The SendFinalStatus is
        **   responsible for writing the final status response to the client. The CloseOutRequest
        **   is executed after the write to the client completes and it is responsible for
        **   cleaning up the Request and its associated connection.
        **   Once the cleanup is performed, then the RequestContext is added back to the free list so
        **   it can be used to handle a new request.
         */
        sendFinalStatus = new SendFinalStatus(this, memoryManager);
        requestHandlerOperations.put(sendFinalStatus.getOperationType(), sendFinalStatus);
        clientWritePtr = sendFinalStatus.initialize();

        closeRequest = new CloseOutRequest(this, memoryManager, clientWritePtr);
        requestHandlerOperations.put(closeRequest.getOperationType(), closeRequest);
        clientWriteDonePtr = closeRequest.initialize();


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
        DetermineRequestType determineRequestType = new DetermineRequestType(this, supportedHttpRequests);
        requestHandlerOperations.put(determineRequestType.getOperationType(), determineRequestType);
        determineRequestType.initialize();

        ParseHttpRequest httpParser = new ParseHttpRequest(this, readPtr, metering, determineRequestType);
        requestHandlerOperations.put(httpParser.getOperationType(), httpParser);
        httpParser.initialize();
    }

    /*
    ** This is used to cleanup the HTTP Request parsing Operations and their dependencies
     */
    public void cleanupHttpParser() {
        Operation operation;

        operation = requestHandlerOperations.get(OperationTypeEnum.PARSE_HTTP_BUFFER);
        operation.complete();
        requestHandlerOperations.remove(OperationTypeEnum.PARSE_HTTP_BUFFER);

        operation = requestHandlerOperations.get(OperationTypeEnum.DETERMINE_REQUEST_TYPE);
        operation.complete();
        requestHandlerOperations.remove(OperationTypeEnum.DETERMINE_REQUEST_TYPE);
    }

    /*
    ** This is called when the request has completed and this RequestContext needs to be put back into
    **   a pristine state so it can be used for a new request.
    ** Once it is cleaned up, this RequestContext is added back to the free list so it can be used again.
     */
    public void cleanupRequest() {
        Operation operation;

        operation = requestHandlerOperations.get(OperationTypeEnum.SEND_FINAL_STATUS);
        operation.complete();

        operation = requestHandlerOperations.get(OperationTypeEnum.REQUEST_FINISHED);
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

    }

    /*
    ** This is the method that is called to execute all of the Operations that have been
    **   placed in the "ready to run" state as the result of their "event" being triggered.
    ** This is the method that is called by the Event Thread responsible for this request.
     */
    public void performWork() {
        List<Operation> operationsToRun = new ArrayList<>();

        try {
            queueMutex.lock();
            try {
                if (workQueued || queueSignal.await(100, TimeUnit.MILLISECONDS)) {
                    int drainedCount = workQueue.drainTo(operationsToRun, MAX_EXEC_WORK_LOOP_COUNT);
                    for (Operation operation : operationsToRun) {
                        //LOG.info("ConnectionState[" + connState.getConnStateId() + "] pulled from workQueue");
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
            //LOG.info("RequestContext[" + connectionRequestId + "] addToWorkQueue() onExecutionQueue: " + work.isOnWorkQueue() +
            //        " onTimedWaitQueue: " + work.isOnTimedWaitQueue());

            if (!operation.isOnWorkQueue()) {
                if (operation.isOnTimedWaitQueue()) {
                    if (timedWaitQueue.remove(operation)) {
                        operation.markRemovedFromQueue(true);
                    } else {
                        LOG.error("RequestContext[" + connectionRequestId + "] addToWorkQueue() not on timedWaitQueue");
                    }
                }

                if (!workQueue.offer(operation)) {
                    LOG.error("RequestContext[" + connectionRequestId + "] addToWorkQueue() unable to add");
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
            //LOG.info("RequestContext[" + connectionRequestId + "] addToDelayedQueue() onExecutionQueue: " + operation.isOnWorkQueue() +
            //        " onTimedWaitQueue: " + operation.isOnTimedWaitQueue());

            if (!operation.isOnWorkQueue() && !operation.isOnTimedWaitQueue()) {
                if (!timedWaitQueue.offer(operation)) {
                    LOG.error("RequestContext[" + connectionRequestId + "] addToWorkQueue() unable to add");
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
                //LOG.info("RequestContext[" + connectionRequestId + "] removeFromQueue() workQueue");
                operation.markRemovedFromQueue(false);
            } else if (timedWaitQueue.remove(operation)) {
                //LOG.info("RequestContext[" + connectionRequestId + "] removeFromQueue() timeWaitQueue");
                operation.markRemovedFromQueue(true);
            } else {
                LOG.warn("RequestContext[" + connectionRequestId + "] removeFromQueue() not on any queue");
            }
        } finally {
            queueMutex.unlock();
        }
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


    public void httpHeaderParseComplete(final long contentLength) {
        requestContentLength = contentLength;
    }

    public boolean isHttpRequestParsed() {
        return httpRequestParsed;
    }

    public boolean getHttpParseError() {
        return httpParseError;
    }
}
