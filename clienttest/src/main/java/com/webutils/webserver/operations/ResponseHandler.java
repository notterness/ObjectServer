package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.http.parser.ResponseHttpParser;
import com.webutils.webserver.manual.HttpResponseCompleted;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ResponseHandler implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseHandler.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.RESPONSE_HANDLER;

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
     ** The following are the BufferManager and pointers that are used to read in the HTTP Response from
     **   the Object Server and to process it.
     */
    private final BufferManager responseBufferManager;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer httpResponseBufferPointer;

    /*
     ** The metering operation to hand out buffers to read in the response header and the data
     */
    private final BufferReadMetering metering;

    /*
     ** The HTTP Parser is used to extract the status from the Storage Server response
     */
    private ResponseHttpParser parser;
    private boolean initialHttpBuffer;

    /*
     ** The completionCallback is what is used to call back into the SetupChunkWrite to indicate that the
     **   response has been received (good or bad) from the Storage Server
     */
    private final Operation completionCallback;

    private final HttpResponseInfo httpInfo;

    public ResponseHandler(final RequestContext requestContext, final BufferManager responseBufferMgr,
                           final BufferManagerPointer readBufferPtr, final BufferReadMetering metering,
                           final Operation completionCb, final ServerIdentifier objectServer) {

        this.requestContext = requestContext;

        this.responseBufferManager = responseBufferMgr;
        this.readBufferPointer = readBufferPtr;

        this.metering = metering;

        this.completionCallback = completionCb;

        this.httpInfo = objectServer.getHttpInfo();

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
        httpResponseBufferPointer = responseBufferManager.register(this, readBufferPointer);

        HttpResponseCompleted httpResponseCompleted = new HttpResponseCompleted(null);

        parser = new ResponseHttpParser(httpInfo, httpResponseCompleted);
        initialHttpBuffer = true;

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

        while ((httpBuffer = responseBufferManager.peek(httpResponseBufferPointer)) != null) {
            /*
             ** Now run the Buffer State through the Http Parser
             */
            boolean remainingBuffer = parser.parseHttpData(httpBuffer, initialHttpBuffer);
            if (remainingBuffer) {
                /*
                 ** Leave the pointer in the same place since there is data remaining in the buffer
                 */
                //LOG.info("ResponseHandler[" + requestContext.getRequestId() + "] remaining position: " +
                //        httpBuffer.position() + " limit: " + httpBuffer.limit());

            } else {
                /*
                 ** Only update the pointer if the data in the buffer was all consumed.
                 */
                //LOG.info("ResponseHandler[" + requestContext.getRequestId() + "]  position: " +
                //        httpBuffer.position() + " limit: " + httpBuffer.limit());

                responseBufferManager.updateConsumerReadPointer(httpResponseBufferPointer);
            }

            initialHttpBuffer = false;

            /*
             ** Need to break out of the loop if the parsing is complete.
             */
            if (httpInfo.getHeaderComplete()) {
                break;
            }
        }

        /*
         ** Check if there needs to be another read to bring in more of the HTTP request
         */
        boolean headerParsed = httpInfo.getHeaderComplete();
        if (!headerParsed) {
            /*
             ** Allocate another buffer and read in more data. But, do not
             **   allocate if there was a parsing error.
             */
            if (!requestContext.getHttpParseError()) {
                /*
                 ** Meter out another buffer here.
                 */
                metering.event();
            } else {
                LOG.info("ResponseHandler[" + requestContext.getRequestId() + "] parsing error, no allocation");

                completionCallback.event();
            }
        } else {
            LOG.info("ResponseHandler[" + requestContext.getRequestId() + "] header was parsed");

            /*
             ** Create a book mark for the next set of readers to register against.
             */
            responseBufferManager.bookmark(httpResponseBufferPointer);

            /*
             ** Send the event to the next operation to allow this request to proceed.
             */
            completionCallback.event();
        }

    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {
        responseBufferManager.unregister(httpResponseBufferPointer);
        httpResponseBufferPointer = null;

        /*
         ** The ResponseHttpParser is also no longer needed so remove any references to it
         */
        parser = null;
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
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ResponseHandler[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ResponseHandler[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ResponseHandler[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ResponseHandler[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ResponseHandler[" + requestContext.getRequestId() +
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
