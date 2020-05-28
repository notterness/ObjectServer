package com.webutils.webserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ConvertRespBodyToString implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertRespBodyToString.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.CONVERT_RESP_BODY_TO_STR;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final ClientRequestContext requestContext;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     */
    private final BufferManager clientReadBufferMgr;

    /*
     ** The clientFullBufferPtr is used to track ByteBuffer(s) that are filled with client object data and are
     **   ready to be written out to the file on disk for later comparison.
     */
    private final BufferManagerPointer readBufferPointer;

    private final Operation readBufferMetering;

    private final HttpResponseInfo httpInfo;

    /*
     ** The following operations complete() method will be called when this operation and it's
     **   dependent operation are finished. This allows the upper layer to clean up and
     **   release any resources.
     */
    private final Operation completeCallback;

    private BufferManagerPointer respBodyReadPointer;

    /*
     ** The following are used to keep track of how much has been written to this Storage Server and
     **   how much is supposed to be written.
     */
    private final int bytesToConvert;
    private int bytesConverted;

    private int savedSrcPosition;

    private String respBodyStr;

    /*
     */
    public ConvertRespBodyToString(final ClientRequestContext requestContext, final BufferManagerPointer readBufferPtr,
                                   final Operation metering, final HttpResponseInfo httpInfo, final Operation completeCb) {

        this.requestContext = requestContext;
        this.clientReadBufferMgr = requestContext.getClientReadBufferManager();

        this.httpInfo = httpInfo;

        this.completeCallback = completeCb;

        this.readBufferPointer = readBufferPtr;

        this.readBufferMetering = metering;
        this.bytesToConvert = httpInfo.getContentLength();

        this.respBodyStr = null;

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
     ** This returns the BufferManagerPointer obtained by this operation, if there is one. If this operation
     **   does not use a BufferManagerPointer, it will return null.
     */
    public BufferManagerPointer initialize() {
        /*
         ** This keeps track of the number of bytes that have been converted to a String
         **   need to be written.
         */
        bytesConverted = 0;

        /*
         ** Register this with the Buffer Manager to allow it to be event(ed) when
         **   buffers are added by the read producer.
         **
         ** This operation (ConvertRespBodyToString) is a consumer of ByteBuffer(s) produced by the ReadBuffer operation.
         */
        respBodyReadPointer = clientReadBufferMgr.register(this, readBufferPointer);

        /*
         ** savedSrcPosition is used to handle the case where there are multiple readers from the readBufferPointer and
         **   there has already been data read from the buffer. In that case, the position() will not be zero, but there
         **   is a race condition as to how the cursors within the "base" buffer are adjusted. The best solution is to
         **   use a "copy" of the buffer and to set its cursors appropriately.
         */
        ByteBuffer readBuffer;
        if ((readBuffer = clientReadBufferMgr.peek(respBodyReadPointer)) != null) {
            savedSrcPosition = readBuffer.position();

            /*
             ** Add this to the execute queue since there is already data in a buffer to decrypt
             */
            event();
        } else {
            savedSrcPosition = 0;

            /*
             ** Need to provide a buffer to the READ_BUFFER operation to allow the NioSocket to read in the next data
             **   bytes.
             */
            readBufferMetering.event();
        }

        LOG.info("ConvertRespBodyToString initialize done savedSrcPosition: " + savedSrcPosition);

        return respBodyReadPointer;
    }

    /*
     ** The WriteToBuffer operation will have its "event()" method invoked whenever there is data read into
     **   the ClientReadBufferManager.
     */
    public void event() {
        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     ** This method will go through all of the available buffers in the ClientReadBufferManager that have data
     **   in them and encrypt that data and put it into the StorageServerWriteBufferManager.
     ** There are two cases to consider for the loop:
     **   -> Are the ByteBuffer(s) in the ClientReadBufferManager that have data within them and are ready to be
     **        encrypted.]
     **   -> Are there ByteBuffer(s) in the StorageServerWriteBufferManager that are available. There is always the
     **        case where all the buffers in the StorageServerWriteBufferManager are waiting to be written to one or
     **        more Storage Servers and everything is going to back up until the writes start completing and making
     **        buffers available.
     */
    public void execute() {
        ByteBuffer readBuffer;
        boolean outOfBuffers = false;

        while (!outOfBuffers) {
            if ((readBuffer = clientReadBufferMgr.peek(respBodyReadPointer)) != null) {
                /*
                 ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
                 **  affecting the position() and limit() indexes
                 **
                 ** NOTE: Must reset the savedSrcPosition after reading in the first ByteBuffer is handled otherwise
                 **   the counts and positions get all screwed up.
                 */
                ByteBuffer srcBuffer = readBuffer.duplicate();
                srcBuffer.position(savedSrcPosition);
                savedSrcPosition = 0;

                LOG.info("ConvertRespBodyToStr bytesToConvert: " + bytesToConvert + " bytesConverted: " + bytesConverted +
                        " remaining: " + srcBuffer.remaining());
                bytesConverted += srcBuffer.remaining();

                String tmpStr = HttpInfo.bb_to_str(srcBuffer);

                //LOG.info("ConvertRespBodyToStr tmpStr: " + tmpStr);
                if (respBodyStr == null) {
                    respBodyStr = tmpStr;
                } else {
                    respBodyStr += tmpStr;
                }

                clientReadBufferMgr.updateConsumerReadPointer(respBodyReadPointer);
            } else {
                LOG.info("ConvertRespBodyToString out of read buffers bytesConverted: " +
                        bytesConverted + " bytesToConvert: " + bytesToConvert);

                /*
                 ** Check if all the bytes (meaning the amount passed in the content-length in the HTTP header)
                 **   have been written to the file. If not, dole out another ByteBuffer to the NIO read
                 **   operation.
                 */
                if (bytesConverted < bytesToConvert) {
                    readBufferMetering.event();
                } else if (bytesConverted == bytesToConvert) {

                    httpInfo.setResponseBody(respBodyStr);

                    /*
                     ** Done with this operation, so set the flag within the RequestContext. The SetupStorageServerPut
                     **   operation is dependent upon this write of the data to disk completing as well as the Md5 digest
                     **   being completed before it can send status back to the Object Server.
                     ** Tell the SetupStorageServerPut operation that produced this that it is done.
                     */
                    requestContext.setAllObjectDataWritten();

                    completeCallback.event();
                }

                outOfBuffers = true;
            }
        }
    }

    /*
     */
    public void complete() {
        LOG.info("ConvertRespBodyToString complete");

        clientReadBufferMgr.unregister(respBodyReadPointer);
        respBodyReadPointer = null;
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
        //LOG.info("ConvertRespBodyToString markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ConvertRespBodyToString markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ConvertRespBodyToString markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ConvertRespBodyToString markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ConvertRespBodyToString hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }


    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        respBodyReadPointer.dumpPointerInfo();
        LOG.info("");
    }

 }
