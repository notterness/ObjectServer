package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.http.HttpResponseListener;
import com.oracle.athena.webserver.http.StorageServerResponseCallback;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class StorageServerResponseHandler implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(StorageServerResponseHandler.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.STORAGE_SERVER_RESPONSE_HANDLER;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;


    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
    ** The following are the BufferManager and pointers that are used to read in the HTTP Response from
    **   the Storage Server and to process it.
     */
    private final BufferManager storageServerResponseBufferManager;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer httpResponseBufferPointer;

    /*
    ** The HTTP Parser is used to extract the status from the Storage Server response
     */
    private HttpParser httpParser;

    /*
    ** The completionCallback is what is used to call back into the SetupChunkWrite to indicate that the
    **   response has been received (good or bad) from the Storage Server
     */
    private final Operation completionCallback;


    public StorageServerResponseHandler(final RequestContext requestContext, final BufferManager storageServerResponseBufferMgr,
                                 final BufferManagerPointer readBufferPtr, final Operation completionCb) {

        this.requestContext = requestContext;

        this.storageServerResponseBufferManager = storageServerResponseBufferMgr;
        this.readBufferPointer = readBufferPtr;

        this.completionCallback = completionCb;

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
     */
    public BufferManagerPointer initialize() {
        /*
         ** Register this with the Buffer Manager to allow it to be event(ed) when
         **   buffers are added by the read producer
         */
        httpResponseBufferPointer = storageServerResponseBufferManager.register(this, readBufferPointer);

        StorageServerResponseCallback httpResponseCompleted = new StorageServerResponseCallback(completionCallback);
        HttpResponseListener listener = new HttpResponseListener(httpResponseCompleted);
        httpParser = new HttpParser(listener);

        if (httpParser.isState(HttpParser.State.END))
            httpParser.reset();
        if (!httpParser.isState(HttpParser.State.START))
            throw new IllegalStateException("!START");

        return httpResponseBufferPointer;
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
        ByteBuffer httpBuffer;

        while ((httpBuffer = storageServerResponseBufferManager.peek(httpResponseBufferPointer)) != null) {
            /*
             ** Now run the Buffer State through the Http Parser
             */
            httpBuffer.flip();

            httpParser.parseNext(httpBuffer);
            storageServerResponseBufferManager.updateConsumerReadPointer(httpResponseBufferPointer);
        }

    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {
        httpParser = null;

        storageServerResponseBufferManager.unregister(httpResponseBufferPointer);
        httpResponseBufferPointer = null;
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
