package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpResponseListener;
import com.webutils.webserver.manual.ClientTest;
import com.webutils.webserver.manual.HttpResponseCompleted;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ClientResponseHandler implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ClientResponseHandler.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_RESPONSE_HANDLER;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The
     */
    private final ClientTest clientTest;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    private final BufferManager clientReadBufferManager;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer httpResponseBufferPointer;

    private HttpParser httpParser;

    public ClientResponseHandler(final RequestContext requestContext, final ClientTest clientTest,
                                 final BufferManagerPointer readBufferPtr) {

        this.requestContext = requestContext;
        this.clientTest = clientTest;
        this.readBufferPointer = readBufferPtr;

        this.clientReadBufferManager = requestContext.getClientReadBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
     */
    public BufferManagerPointer initialize() {
        /*
         ** Register this with the Buffer Manager to allow it to be event(ed) when
         **   buffers are added by the read producer
         */
        httpResponseBufferPointer = clientReadBufferManager.register(this, readBufferPointer);

        HttpResponseCompleted httpResponseCompleted = new HttpResponseCompleted(clientTest);
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

        while ((httpBuffer = clientReadBufferManager.peek(httpResponseBufferPointer)) != null) {
            /*
             ** Now run the Buffer State through the Http Parser
             */
            httpParser.parseNext(httpBuffer);
            clientReadBufferManager.updateConsumerReadPointer(httpResponseBufferPointer);
        }

    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {

        LOG.info("ClientResponseHandler[" + requestContext.getRequestId() + "] complete()");
        httpParser.reset();
        httpParser = null;

        clientReadBufferManager.unregister(httpResponseBufferPointer);
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
        //LOG.info("ClientResponseHandler[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ClientResponseHandler[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ClientResponseHandler[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ClientResponseHandler[" + requestContext.getRequestId() + "] markAddToQueue(" +
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
        LOG.warn("ClientResponseHandler[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
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
