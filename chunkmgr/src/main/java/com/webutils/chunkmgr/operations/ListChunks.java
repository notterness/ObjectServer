package com.webutils.chunkmgr.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.ListChunkInfo;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerChunkMgr;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class ListChunks implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(ListChunks.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.LIST_CHUNKS;

    /*
     ** The following are used to build the response header
     */
    private final static String SUCCESS_HEADER_1 = "opc-client-request-id: ";
    private final static String SUCCESS_HEADER_2 = "opc-request-id: ";
    private final static String SUCCESS_HEADER_8 = "Content-Length: ";

    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    private final Operation completeCallback;

    private final List<ListChunkInfo> chunks;

    /*
     ** Used to insure that the database operations to list the chunks does not get run multiple times.
     */
    private boolean listCreated;

    /*
     ** The clientWriteBufferMgr is used to queue up writes back to the client. In this case, the
     **   writes are to send the final request status.
     */
    private final BufferManager clientWriteBufferMgr;
    private BufferManagerPointer writeStatusBufferPtr;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onExecutionQueue;

    /*
     ** ListChunks has the following optional headers:
     **   storageTier - List the chunks for a particular storage tier
     **   storage-server-name - This may be a string to match against. For example "server-xyx-*". This will return all
     **     the chunks associated with the matching storage server(s).
     **   chunk-status - This can be AVAILABLE, ALLOCATED or DELETED.
     **   limit - The maximum number of items to return
     */
    public ListChunks(final RequestContext requestContext, final MemoryManager memoryManager, final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;

        this.completeCallback = completeCb;

        this.listCreated = false;

        this.chunks = new LinkedList<>();

        this.clientWriteBufferMgr = this.requestContext.getClientWriteBufferManager();

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

        writeStatusBufferPtr = clientWriteBufferMgr.register(this);

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
     */
    public void execute() {
        if (!listCreated) {
            ServerChunkMgr chunkMgrMgr = new ServerChunkMgr(requestContext.getWebServerFlavor());

            int status = chunkMgrMgr.listChunks(requestContext.getHttpInfo(), chunks);
            if (status == HttpStatus.OK_200) {
                sendHeaderAndResponse();
            } else {
                LOG.warn("ListChunks failed status: " + status);
            }

            listCreated = true;
        }
    }

    public void complete() {
        /*
         ** Release any buffers in the clientWriteBufferMgr that were used to send the response headers and content for
         **   the ListChunks method.
         */
        int bufferCount = writeStatusBufferPtr.getCurrIndex();

        LOG.info("ListChunks[" + requestContext.getRequestId() + "] complete() bufferCount: " + bufferCount);

        clientWriteBufferMgr.reset(writeStatusBufferPtr);
        for (int i = 0; i < bufferCount; i++) {
            ByteBuffer buffer = clientWriteBufferMgr.getAndRemove(writeStatusBufferPtr);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer, clientWriteBufferMgr);
            } else {
                LOG.info("ListChunks[" + requestContext.getRequestId() + "] missing buffer i: " + i);
            }
        }

        clientWriteBufferMgr.unregister(writeStatusBufferPtr);
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
        //LOG.info("ListChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("ListChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("ListChunks[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("ListChunks[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("ListChunks[" + requestContext.getRequestId() + "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
    }

    /*
     ** This builds the OK_200 response headers for the GET ListObjects method. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     **   opc-request-id
     */
    private void buildSuccessHeader(final ByteBuffer respBuffer, final int contentLength) {
        String successHeader;

        HttpRequestInfo requestInfo = requestContext.getHttpInfo();
        String opcClientRequestId = requestInfo.getOpcClientRequestId();
        int opcRequestId = requestInfo.getRequestId();

        successHeader = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/html\n";
        if (opcClientRequestId != null) {
            successHeader += SUCCESS_HEADER_1 + opcClientRequestId + "\n" + SUCCESS_HEADER_2 + opcRequestId + "\n";
        } else {
            successHeader += SUCCESS_HEADER_2 + opcRequestId + "\n";
        }

        successHeader += SUCCESS_HEADER_8 + contentLength + "\n\n";

        HttpInfo.str_to_bb(respBuffer, successHeader);
    }

    /*
     ** This builds the error response headers for the GET Object command. This returns the following headers:
     **
     **   opc-client-request-id - If the client passed one in, otherwise it it will not be returned
     */
    private void buildFailureHeaders(final ByteBuffer respBuffer) {
        String failureHeader;

        HttpRequestInfo httpInfo = requestContext.getHttpInfo();

        String opcClientRequestId = httpInfo.getOpcClientRequestId();
        int opcRequestId = requestContext.getRequestId();

        LOG.info("buildFailureHeader()  opc-client-request-id: " + opcClientRequestId);

        if (opcClientRequestId != null) {
            failureHeader = "HTTP/1.1 " + httpInfo.getParseFailureCode() + " FAILED\r\n" +
                    "Content-Type: text/html\n" + SUCCESS_HEADER_1 + opcClientRequestId + "\n";
        } else {
            failureHeader = "HTTP/1.1 " + httpInfo.getParseFailureCode() + " FAILED\r\n" +
                    "Content-Type: text/html\n";
        }

        failureHeader += SUCCESS_HEADER_2 + opcRequestId + "\n";

        String failureMessage = httpInfo.getParseFailureReason();
        if (failureMessage != null) {
            failureHeader += SUCCESS_HEADER_8 + failureMessage.length() + "\n\n";
            failureHeader += failureMessage;
        } else {
            failureHeader += SUCCESS_HEADER_8 + 0 + "\n\n";
        }

        HttpInfo.str_to_bb(respBuffer, failureHeader);
    }

    private void sendHeaderAndResponse() {
        /*
        ** Build the FIFO of String(s) that represent the response. This is not the ideal way to do this as the
        **   creation of the String(s) consumes memory, but there is no real easy way to compute the required
        **   "Content-Length" without knowing how much data there is to send. Ideally (need to investigate) there
        **   is a concept setup for streaming data that does not require the "Content-Length" to be known
        **   prior to sending the data.
         */
        Queue<String> respQueue = new LinkedBlockingQueue<>();

        int chunkCount = chunks.size();
        int currChunk = 0;
        int contentLength = 0;
        for (ListChunkInfo info: chunks) {
            currChunk++;

            String tmp = info.buildChunkListResponse((currChunk == 1), (currChunk == chunkCount));
            contentLength += tmp.length();
            respQueue.add(tmp);
        }
        chunks.clear();

        /*
        ** There are two parts to send out, first the header and then the content data.
         */
        ByteBuffer respBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, clientWriteBufferMgr,
                    operationType);
        if (respBuffer != null) {

            if (requestContext.getHttpParseStatus() == HttpStatus.OK_200) {
                LOG.info("ListChunks[" + requestContext.getRequestId() + "] resultCode: OK_200");

                buildSuccessHeader(respBuffer, contentLength);
            } else {
                LOG.info("ListChunks[" + requestContext.getRequestId() + "] resultCode: " +
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
            LOG.warn("ListChunks[" + requestContext.getRequestId() + "] unable to allocate response buffer");

            /*
            ** Go right to the CloseOutRequest operation. That will close out the connection.
             */
            return;
        }

        /*
        ** Do not send the content if there was an error response sent back
         */
        if (requestContext.getHttpParseStatus() == HttpStatus.OK_200) {
            /*
             ** Now need to send the content
             **
             ** NOTE: This makes an assumption that any single String pulled from the queue is always going to fit within
             **   an allocated ByteBuffer. If that is not the case, then the code will need to handle splitting the
             **   String across multiple buffers.
             */
            String lastEntry = null;
            while (contentLength != 0) {
                respBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, clientWriteBufferMgr, operationType);
                if (respBuffer != null) {
                    if (lastEntry != null) {
                        HttpInfo.str_to_bb(respBuffer, lastEntry);
                        contentLength -= lastEntry.length();
                    }
                    //LOG.info("sendHeaderAndResponse(1) position: " + respBuffer.position() + " remaining: " + respBuffer.remaining());

                    while (true) {
                        lastEntry = respQueue.poll();
                        if (lastEntry != null) {
                            int length = lastEntry.length();
                            //LOG.info("sendHeaderAndResponse(2) length: " + length + " position: " + respBuffer.position() +
                            //        " remaining: " + respBuffer.remaining());

                            if (respBuffer.remaining() > length) {
                                HttpInfo.str_to_bb(respBuffer, lastEntry);
                                contentLength -= length;
                            } else {
                                /*
                                 ** Mark this buffer as ready to be written by adding it to the queue and then exit the loop
                                 **   to cause another buffer to be allocated (if there is remaining contentLength).
                                 */
                                respBuffer.flip();
                                clientWriteBufferMgr.offer(writeStatusBufferPtr, respBuffer);
                                break;
                            }

                            //LOG.info("sendHeaderAndResponse(2) position: " + respBuffer.position() + " remaining: " + respBuffer.remaining());
                        } else {
                            /*
                             ** Out of String(s) to write. The contentLength should be zero at this point.
                             */
                            //LOG.info("sendHeaderAndResponse(3) contentLength: " + contentLength);
                            respBuffer.flip();
                            clientWriteBufferMgr.offer(writeStatusBufferPtr, respBuffer);
                            break;
                        }
                    }
                } else {
                    LOG.warn("ListChunks[" + requestContext.getRequestId() + "] unable to allocate response buffer");
                    break;
                }
            }
        }

        respQueue.clear();

        /*
        ** Mark that the content has all been queued up to the write process
         */
        requestContext.setAllClientBuffersFilled();
    }
}
