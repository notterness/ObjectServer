package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.parser.PostContentParser;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ParseContentBuffers implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ParseContentBuffers.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.PARSE_CONTENT;

    private final RequestContext requestContext;

    private final BufferManager readBufferManager;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer postContentPointer;

    private final Operation meteringOperation;
    private final Operation completeCallback;

    private final PostContentParser parser;

    private final ContentParser contentParser;

    private int savedSrcPosition;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public ParseContentBuffers(final RequestContext requestContext, final BufferManager readBufferMgr,
                               final BufferManagerPointer readBufferPtr, final Operation metering,
                               final ContentParser contentParser, final int contentLength,
                               final Operation completeCb) {

        this.requestContext = requestContext;
        this.readBufferPointer = readBufferPtr;
        this.meteringOperation = metering;
        this.contentParser = contentParser;
        this.completeCallback = completeCb;

        this.readBufferManager = readBufferMgr;

        /*
         ** Setup the parser to pull the content information out of the request
         */
        parser = new PostContentParser(contentLength, contentParser);

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
    }

    public ParseContentBuffers(final RequestContext requestContext, final BufferManagerPointer readBufferPtr,
                               final Operation metering, final ContentParser contentParser, final Operation completeCb) {

        this(requestContext, requestContext.getClientReadBufferManager(), readBufferPtr, metering, contentParser,
                requestContext.getRequestContentLength(), completeCb);
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    public BufferManagerPointer initialize() {

        /*
         ** Register this with the Buffer Manager to allow it to be event(ed) when
         **   buffers are added by the read producer
         */
        postContentPointer = readBufferManager.register(this, readBufferPointer);

        /*
         ** savedSrcPosition is used to handle the case where there are no buffers available to place
         **   response data into, so this operation will need to wait until buffers are available.
         */
        ByteBuffer readBuffer;
        if ((readBuffer = readBufferManager.peek(postContentPointer)) != null) {
            savedSrcPosition = readBuffer.position();
            event();
        } else {
            savedSrcPosition = 0;
        }

        LOG.info("ParseContentBuffers savedSrcPosition: " + savedSrcPosition);

        return postContentPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     ** The parse HTTP Request assumes that there is only a single buffer available at any time.
     **
     ** TODO: To remove the requirement that buffers are handled one at a time, the while() loop
     **   would need to check for the header parsed boolean.
     */
    public void execute() {
        ByteBuffer contentBuffer;
        boolean success;

        while ((contentBuffer = readBufferManager.peek(postContentPointer)) != null) {

            /*
             ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
             **  affecting the position() and limit() indexes
             */
            ByteBuffer srcBuffer = contentBuffer.duplicate();
            srcBuffer.position(savedSrcPosition);
            savedSrcPosition = 0;

            //LOG.info("ParseContentBuffers[" + requestContext.getRequestId() + "] remaining position: " +
            //        srcBuffer.position() + " limit: " + srcBuffer.limit());

            /*
             ** Now run the Buffer State through the POST Content Parser
             */
            success = parser.parseBuffer(srcBuffer);
            if (!success) {
                requestContext.getHttpInfo().setParseFailureCode(HttpStatus.BAD_REQUEST_400);
            }

            /*
            ** Update the pointer since all the data in the buffer had to have been parsed
            */
            readBufferManager.updateConsumerReadPointer(postContentPointer);

            /*
             ** Need to break out of the loop if the parsing is complete.
             */
            if (parser.allContentParsed()) {
                break;
            }
        }

        //LOG.info("ParseContentBuffers[" + requestContext.getRequestId() + "] exit from loop");

        /*
         ** Check if there needs to be another read to bring in more of the HTTP request
         */
        if (!parser.allContentParsed()) {
            /*
             ** Allocate another buffer and read in more data. But, do not
             **   allocate if there was a parsing error.
             */
            if (!requestContext.getHttpParseError()) {
                /*
                 ** Meter out another buffer here.
                 */
                meteringOperation.event();
            } else {
                LOG.warn("ParseContentBuffers[" + requestContext.getRequestId() + "] parsing error, no allocation");

                /*
                 ** Event the DetermineRequest. This will check if there is an error and then perform the
                 **   setup for the send of the final status to the client.
                 */
                completeCallback.event();
            }
        } else {
            /*
            ** Even if there is an error, the content data has all been parsed (or at least as far as it will be).
             */
            requestContext.setPostMethodContentParsed();

            /*
            ** Make sure that there was not a parsing error up until this point. Assuming it was all good, then validate
            **   that the required attributes are all present.
             */
            if (!parser.getParseError()) {
                if (contentParser.validateContentData()) {
                    contentParser.dumpMaps();
                } else {
                    /*
                    ** Some required attributes are missing. A further enhancement would be to return the missing
                    **   attributes in the payload.
                     */
                    requestContext.getHttpInfo().setParseFailureCode(HttpStatus.BAD_REQUEST_400);
                }
            } else {
                LOG.warn("ParseContentBuffers[" + requestContext.getRequestId() + "] content parser error");
            }

            /*
             ** Send the event to the completion callback operation to allow this request to proceed.
             */
            completeCallback.event();
        }
    }

    public void complete() {
        LOG.info("ParseContentBuffers[" + requestContext.getRequestId() + "] complete");

        /*
         ** Since the HTTP Request parsing is done for this request, need to remove the dependency on the
         **   read buffer BufferManager pointer to stop events from being generated.
         */
        readBufferManager.unregister(postContentPointer);
        postContentPointer = null;
    }

    /*
     ** The following are used to add the Operation to the event thread's event queue. The
     **   Operation can be added to the immediate execution queue or the delayed
     **   execution queue.
     **
     ** The following methods are called by the event thread under a queue mutex.
     **   markRemoveFromQueue - This method is used by the event thread to update the queue
     **     the Operation is on when the operation is removed from the queue.
     **   markAddedToQueue - This method is used when an Operation is added to a queue to mark
     **     which queue it is on.
     **   isOnWorkQueue - Accessor method
     **   isOnTimedWaitQueue - Accessor method
     **   hasWaitTimeElapsed - Is this Operation ready to run again to check some timeout condition
     **
     */
    public void markRemovedFromQueue(final boolean delayedExecutionQueue) {
        //LOG.info("ParseContentBuffers[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ParseContentBuffers[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ParseContentBuffers[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ParseContentBuffers[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ParseContentBuffers[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        postContentPointer.dumpPointerInfo();
        LOG.info("");
    }

}
