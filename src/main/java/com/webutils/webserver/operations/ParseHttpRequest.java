package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.parser.ByteBufferHttpParser;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/*
** The ParseHttpRequest is dependent upon having ByteBuffers with valid data to feed into the HTTP Parser.
**
** If the HTTP Parser is looking for more data following it parsing the available buffers, it will fire
**   an event to the BufferReadMetering Operation to make another buffer available for the ReadBuffer
**   operation.
**
** When the HTTP Parser has completed its parsing of the HTTP Request, it will fire an event to the
**   the DetermineRequest operation.
 */

public class ParseHttpRequest implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ParseHttpRequest.class);


    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.PARSE_HTTP_BUFFER;

    /*
     ** There is an ByteBufferHttpParser per Connection since each parser keeps its own state.
     */
    private ByteBufferHttpParser httpParser;

    private final HttpRequestInfo httpRequestInfo;

    private boolean initialHttpBuffer;

    private final RequestContext requestContext;

    private final BufferManager clientReadBufferMgr;
    private final BufferManagerPointer readBufferPointer;
    private BufferManagerPointer httpBufferPointer;

    private final BufferReadMetering meteringOperation;
    private final Operation determineRequestType;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    public ParseHttpRequest(final RequestContext requestContext, final BufferManagerPointer readBufferPtr,
                            final BufferReadMetering metering, final Operation determineRequestType) {

        this.requestContext = requestContext;
        this.readBufferPointer = readBufferPtr;
        this.meteringOperation = metering;
        this.determineRequestType = determineRequestType;

        this.clientReadBufferMgr = this.requestContext.getClientReadBufferManager();

        /*
         ** The HttpInfo keeps track of the details of a particular HTTP transfer and the parsed information.
         */
        this.httpRequestInfo = requestContext.getHttpInfo();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;
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
        httpBufferPointer = clientReadBufferMgr.register(this, readBufferPointer);

        initialHttpBuffer = true;
        httpParser = new ByteBufferHttpParser(httpRequestInfo);

        return httpBufferPointer;
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
        ByteBuffer httpBuffer;

        while ((httpBuffer = clientReadBufferMgr.peek(httpBufferPointer)) != null) {
            /*
             ** Now run the Buffer State through the Http Parser
             */
            boolean remainingBuffer = httpParser.parseHttpData(httpBuffer, initialHttpBuffer);
            if (remainingBuffer) {
                /*
                 ** Leave the pointer in the same place since there is data remaining in the buffer
                 */
                LOG.info("ParseHttpRequest[" + requestContext.getRequestId() + "] remaining position: " +
                        httpBuffer.position() + " limit: " + httpBuffer.limit());

            } else {
                /*
                 ** Only update the pointer if the data in the buffer was all consumed.
                 */
                LOG.info("ParseHttpRequest[" + requestContext.getRequestId() + "]  position: " +
                        httpBuffer.position() + " limit: " + httpBuffer.limit());

                clientReadBufferMgr.updateConsumerReadPointer(httpBufferPointer);
            }

            initialHttpBuffer = false;

            /*
             ** Need to break out of the loop if the parsing is complete.
             */
            if (httpRequestInfo.getHeaderComplete()) {
                break;
            }
        }

        LOG.info("ParseHttpRequest[" + requestContext.getRequestId() + "] exit from loop");

        /*
         ** Check if there needs to be another read to bring in more of the HTTP request
         */
        boolean headerParsed = requestContext.isHttpRequestParsed();
        if (!headerParsed) {
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
                LOG.info("ParseHttpRequest[" + requestContext.getRequestId() + "] parsing error, no allocation");

                /*
                 ** First terminate anything going on with the HTTP Request parser. This will remove any dependencies
                 **   from the BufferManager and cleanup resources that are no longer needed.
                 */
                requestContext.cleanupHttpParser();

                /*
                ** Event the DetermineRequest. This will check if there is an error and then perform the
                **   setup for the send of the final status to the client.
                 */
                determineRequestType.event();
            }
        } else {
            LOG.info("ParseHttpRequest[" + requestContext.getRequestId() + "] header was parsed");

            /*
            ** Create a book mark for the next set of readers to register against.
             */
            clientReadBufferMgr.bookmark(httpBufferPointer);

            /*
             ** First terminate anything going on with the HTTP Request parser. This will remove any dependencies
             **   from the BufferManager and cleanup resources that are no longer needed.
             */
            requestContext.cleanupHttpParser();

            /*
            ** Send the event to the DetermineRequest operation to allow this request to proceed.
             */
            determineRequestType.event();
        }
    }

    public void complete() {
        /*
        ** Since the HTTP Request parsing is done for this request, need to remove the dependency on the
        **   read buffer BufferManager pointer to stop events from being generated.
         */
        clientReadBufferMgr.unregister(httpBufferPointer);
        httpBufferPointer = null;

        /*
        ** The ByteBufferHttpParser is also no longer needed so remove any references to it
         */
        httpParser = null;
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
        //LOG.info("ParseHttpRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ParseHttpRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ParseHttpRequest[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ParseHttpRequest[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ParseHttpRequest[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        httpBufferPointer.dumpPointerInfo();
        LOG.info("");
    }

}
