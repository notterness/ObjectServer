package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.manual.ClientTest;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ClientHttpRequestWrite implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(ClientHttpRequestWrite.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_WRITE_HTTP_HEADER;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
    ** The ClientTest is used to obtain the HTTP Request string
     */
    private final ClientTest clientTest;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
     */
    private final BufferManagerPointer writeInfillPointer;

    /*
    **
     */
    private final ClientObjectWrite clientObjectWrite;
    private final ServerIdentifier serverIdentifier;

    public ClientHttpRequestWrite(final RequestContext requestContext,
                                 final ClientTest clientTest, final BufferManagerPointer writeInfillPtr,
                                 final ClientObjectWrite writeObject, final ServerIdentifier serverIdentifier) {

        this.requestContext = requestContext;
        this.clientTest = clientTest;
        this.writeInfillPointer = writeInfillPtr;
        this.clientObjectWrite = writeObject;
        this.serverIdentifier = serverIdentifier;

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

    public int getRequestId() { return requestContext.getRequestId(); }

    /*
     */
    public BufferManagerPointer initialize() {

        return null;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     ** This will attempt to allocate a ByteBuffer from the free pool and then fill it in with the
     **   response data.
     ** Assuming the B
     */
    public void execute() {

        BufferManager clientWriteBufferMgr = requestContext.getClientWriteBufferManager();

        /*
        ** Build the HTTP Header and the Object to be sent
         */
        ByteBuffer msgHdr = clientWriteBufferMgr.peek(writeInfillPointer);
        if (msgHdr != null) {

            String tmp;
            String Md5_Digest = clientTest.buildBufferAndComputeMd5();
            tmp = clientTest.buildRequestString(Md5_Digest);

            HttpInfo.str_to_bb(msgHdr, tmp);

            /*
            ** Need to flip() the buffer so that the limit() is set to the end of where the HTTP Request is
            **   and the position() reset to 0.
             */
            msgHdr.flip();

            /*
            ** Data is now present in the ByteBuffer so the writeInfillPtr needs to be updated,
             */
            clientWriteBufferMgr.updateProducerWritePointer(writeInfillPointer);

            /*
            ** Need to set that the HTTP Request has been sent to the Web Server to allow the writes of the
            **   object data can take place.
             */
            requestContext.setHttpRequestSent(serverIdentifier);
            clientObjectWrite.event();
        }

    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {

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
        //LOG.info("ClientHttpRequestWrite[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ClientHttpRequestWrite[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ClientHttpRequestWrite[" + requestContext.getRequestId() + "] markRemovedFromQueue(" +
                    delayedExecutionQueue + ") not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ClientHttpRequestWrite[" + requestContext.getRequestId() + "] markAddToQueue(" +
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
        LOG.warn("ClientHttpRequestWrite[" + requestContext.getRequestId() +
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
