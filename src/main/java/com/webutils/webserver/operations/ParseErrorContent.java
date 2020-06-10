package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ParseErrorContent implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ParseErrorContent.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.PARSE_ERROR_CONTENT;

    private final RequestContext requestContext;

    private final BufferManager readBufferManager;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer postContentPointer;

    private final Operation metering;
    private final Operation completeCallback;

    /*
     ** The following are used to keep track of how much has been written to this Storage Server and
     **   how much is supposed to be written.
     */
    private final int bytesToConvert;
    private int bytesConverted;
    private int savedSrcPosition;

    private String respBodyStr;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public ParseErrorContent(final RequestContext requestContext, final BufferManager readBufferMgr,
                               final BufferManagerPointer readBufferPtr, final Operation metering,
                               final int contentLength, final Operation completeCb) {

        this.requestContext = requestContext;
        this.readBufferPointer = readBufferPtr;
        this.metering = metering;
        this.completeCallback = completeCb;

        this.bytesToConvert = contentLength;

        this.readBufferManager = readBufferMgr;

        this.respBodyStr = null;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
    }

    public ParseErrorContent(final RequestContext requestContext, final BufferManagerPointer readBufferPtr,
                             final Operation metering, final Operation completeCb) {

        this(requestContext, requestContext.getClientReadBufferManager(), readBufferPtr, metering,
                requestContext.getRequestContentLength(), completeCb);
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    public int getRequestId() { return requestContext.getRequestId(); }

    public BufferManagerPointer initialize() {
        /*
         ** This keeps track of the number of bytes that have been converted to a String
         **   need to be written.
         */
        bytesConverted = 0;

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

        LOG.info("ParseErrorContent savedSrcPosition: " + savedSrcPosition);

        return postContentPointer;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     ** The parse HTTP Response assumes that there is only a single buffer available at any time.
     **
     */
    public void execute() {
        ByteBuffer contentBuffer;
        boolean outOfBuffers = false;

        while (!outOfBuffers) {
            if ((contentBuffer = readBufferManager.peek(postContentPointer)) != null) {

                /*
                 ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
                 **  affecting the position() and limit() indexes
                 */
                ByteBuffer srcBuffer = contentBuffer.duplicate();
                srcBuffer.position(savedSrcPosition);
                savedSrcPosition = 0;

                LOG.info("ParseErrorContent bytesToConvert: " + bytesToConvert + " bytesConverted: " + bytesConverted +
                        " remaining: " + srcBuffer.remaining());
                bytesConverted += srcBuffer.remaining();

                String tmpStr = HttpInfo.bb_to_str(srcBuffer);

                //LOG.info("ConvertRespBodyToStr tmpStr: " + tmpStr);
                if (respBodyStr == null) {
                    respBodyStr = tmpStr;
                } else {
                    respBodyStr += tmpStr;
                }

                /*
                 ** Update the pointer since all the data in the buffer had to have been parsed
                 */
                readBufferManager.updateConsumerReadPointer(postContentPointer);
            } else {
                LOG.info("ParseErrorContent out of read buffers bytesConverted: " +
                        bytesConverted + " bytesToConvert: " + bytesToConvert);

                /*
                 ** Check if all the bytes (meaning the amount passed in the content-length in the HTTP header)
                 **   have been written to the file. If not, dole out another ByteBuffer to the NIO read
                 **   operation.
                 */
                if (bytesConverted < bytesToConvert) {
                    metering.event();
                } else if (bytesConverted == bytesToConvert) {
                    completeCallback.event();
                }

                outOfBuffers = true;
            }
        }
    }

    public void complete() {
        LOG.info("ParseErrorContent[" + requestContext.getRequestId() + "] complete");

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
        //LOG.info("ParseErrorContent[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ParseErrorContent[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ParseErrorContent[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ParseErrorContent[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ParseErrorContent[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
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
