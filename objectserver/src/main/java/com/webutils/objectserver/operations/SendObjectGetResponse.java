package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.BuildHttpResult;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ObjectInfo;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.SendFinalStatus;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class SendObjectGetResponse implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(SendObjectGetResponse.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.SEND_OBJECT_GET_RESPONSE;

    /*
     ** Strings used to build the success response for the chunk write
     */
    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_3 = "Etag: ";
    private final static String SUCCESS_HEADER_4 = "Content-MD5: ";
    private final static String SUCCESS_HEADER_5 = "last-modified: ";
    private final static String SUCCESS_HEADER_6 = "archival-state: ";
    private final static String SUCCESS_HEADER_7 = "version-id: ";
    private final static String SUCCESS_HEADER_8 = "Content-Length: ";


    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
     ** The MemoryManager is used to allocate a very small number of buffers used to send out the
     **   final status
     */
    private final MemoryManager memoryManager;

    /*
     ** The clientWriteBufferMgr is used to queue up writes back to the client. In this case, the
     **   writes are to send the final request status.
     */
    private BufferManager clientWriteBufferMgr;
    private BufferManagerPointer writeStatusBufferPtr;

    private final ObjectInfo objectInfo;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    private boolean httpResponseSent;


    public SendObjectGetResponse(final RequestContext requestContext, final MemoryManager memoryManager,
                                 final ObjectInfo objectInfo) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.objectInfo = objectInfo;

        this.clientWriteBufferMgr = this.requestContext.getClientWriteBufferManager();

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        httpResponseSent = false;
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
        writeStatusBufferPtr = this.clientWriteBufferMgr.register(this);

        /*
         ** Add a bookmark to insure that the pointer dependent upon this starts at
         **   position 0.
         */
        clientWriteBufferMgr.bookmark(writeStatusBufferPtr);

        return writeStatusBufferPtr;
    }

    public void event() {

        /*
         ** Add this to the execute queue if it is not already on it.
         */
        requestContext.addToWorkQueue(this);
    }

    /*
     ** This will allocate a ByteBuffer from the free pool and then fill it in with the HTTP Response data.
     */
    public void execute() {

        if (!httpResponseSent) {
            ByteBuffer respBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, clientWriteBufferMgr,
                    operationType);
            if (respBuffer != null) {

                if (requestContext.getHttpParseStatus() == HttpStatus.OK_200) {
                    LOG.info("SendObjectGetResponse[" + requestContext.getRequestId() + "] resultCode: OK_200");

                    buildSuccessHeader(respBuffer);
                } else {
                    LOG.info("SendObjectGetResponse[" + requestContext.getRequestId() + "] resultCode: " +
                            requestContext.getHttpParseStatus());

                    buildFailureHeaders(respBuffer);
                }

                respBuffer.flip();

                /*
                 ** Add the ByteBuffer to the clientWriteBufferMgr to kick off the write of the response to the client
                 */
                clientWriteBufferMgr.offer(writeStatusBufferPtr, respBuffer);
            } else {
                /*
                 ** If we are out of memory to allocate a response, might as well close out the connection and give up.
                 */
                LOG.info("SendObjectGetResponse[" + requestContext.getRequestId() + "] unable to allocate response buffer");

                /*
                 ** Go right to the CloseOutRequest operation. That will close out the connection.
                 */

            }

            httpResponseSent = true;

            requestContext.setAllClientBuffersFilled();
        } else {
            LOG.warn("SendObjectGetResponse[" + requestContext.getRequestId() + "] execute() after status sent");
        }
    }

    /*
     ** This removes any dependencies that are put upon the BufferManager
     */
    public void complete() {

        /*
         ** Release any buffers in the clientWriteBufferMgr
         */
        int bufferCount = writeStatusBufferPtr.getCurrIndex();

        LOG.info("SendObjectGetResponse[" + requestContext.getRequestId() + "] complete() bufferCount: " + bufferCount);

        clientWriteBufferMgr.reset(writeStatusBufferPtr);
        for (int i = 0; i < bufferCount; i++) {
            ByteBuffer buffer = clientWriteBufferMgr.getAndRemove(writeStatusBufferPtr);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer, clientWriteBufferMgr);
            } else {
                LOG.info("SendObjectGetResponse[" + requestContext.getRequestId() + "] missing buffer i: " + i);
            }
        }

        clientWriteBufferMgr.unregister(writeStatusBufferPtr);
        writeStatusBufferPtr = null;

        clientWriteBufferMgr = null;
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
        //LOG.info("SendObjectGetResponse[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("SendObjectGetResponse[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("SendObjectGetResponse[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("SendObjectGetResponse[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("SendObjectGetResponse[" + requestContext.getRequestId() +
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

    /*
     ** This builds the OK_200 response headers for the GET Object command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   ETAG
     **   Content-Md5
     **   Last-Modified
     **   Version-Id
     **   Content-Length
     */
    private void buildSuccessHeader(final ByteBuffer respBuffer) {
        String successHeader;

        String opcClientRequestId = requestContext.getHttpInfo().getOpcClientRequestId();
        int opcRequestId = requestContext.getRequestId();
        String etag = objectInfo.getEtag();

        /*
         ** The contentMd5 is saved as part of the object so that the data can be sent prior to the computation being
         **   performed for data being sent back to the client
         */
        String contentMd5 = objectInfo.getContentMd5();

        String lastModified = objectInfo.getLastModified();
        String versionId = objectInfo.getVersionId();

        int contentLength = objectInfo.getContentLength();

        LOG.info("buildSuccessHeader() etag: " + etag + " versionId: " + versionId + " contentLength: " + contentLength);


        if (opcClientRequestId != null) {
            successHeader = "HTTP/1.1 200 OK" +
                    "\r\n" +
                    "Content-Type: text/html\n" + SUCCESS_HEADER_1 + opcClientRequestId + "\n";
        } else {
            successHeader = "HTTP/1.1 200 OK" +
                    "\r\n" +
                    "Content-Type: text/html\n";
        }

        if (contentMd5 != null) {
            successHeader += SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + etag + "\n" +
                    SUCCESS_HEADER_4 + contentMd5 + "\n" + SUCCESS_HEADER_5 + lastModified + "\n" +
                    SUCCESS_HEADER_7 + versionId + "\n" + SUCCESS_HEADER_8 + contentLength + "\n\n";
        } else {
            successHeader += SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + etag + "\n" +
                    SUCCESS_HEADER_5 + lastModified + "\n" +
                    SUCCESS_HEADER_7 + versionId + "\n" + SUCCESS_HEADER_8 + contentLength + "\n\n";
        }

        str_to_bb(respBuffer, successHeader);
    }

    /*
     ** This builds the error response headers for the GET Object command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     **   ETAG
     **   Content-Length - 0
     */
    private void buildFailureHeaders(final ByteBuffer respBuffer) {
        String failureHeader;

        HttpRequestInfo httpInfo = requestContext.getHttpInfo();

        String opcClientRequestId = httpInfo.getOpcClientRequestId();
        int opcRequestId = requestContext.getRequestId();
        String etag = objectInfo.getEtag();

        LOG.info("buildFailureHeader() etag: " + etag + " opc-client-request-id: " + opcClientRequestId +
                " opc-request-id: " + opcRequestId);

        if (opcClientRequestId != null) {
            failureHeader = "HTTP/1.1 " + httpInfo.getParseFailureCode() + " FAILED\r\n" +
                    "Content-Type: text/html\n" + SUCCESS_HEADER_1 + opcClientRequestId + "\n";
        } else {
            failureHeader = "HTTP/1.1 " + httpInfo.getParseFailureCode() + " FAILED\r\n" +
                    "Content-Type: text/html\n";
        }

        String failureMessage = httpInfo.getParseFailureReason();
        if (failureMessage != null) {
            failureHeader += SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + etag + "\n" +
                    SUCCESS_HEADER_8 + failureMessage.length() + "\n\n";
            failureHeader += failureMessage;
        } else {
            failureHeader += SUCCESS_HEADER_2 + opcRequestId + "\n" + SUCCESS_HEADER_3 + etag + "\n" +
                    SUCCESS_HEADER_8 + 0 + "\n\n";
        }

        str_to_bb(respBuffer, failureHeader);
    }

    private void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            boolean endOfInput = true;

            encoder.encode(CharBuffer.wrap(in), out, endOfInput);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
