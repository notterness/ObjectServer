package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class BuildHeaderToStorageServer implements Operation {
    private static final Logger LOG = LoggerFactory.getLogger(BuildHeaderToStorageServer.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.BUILD_HEADER_TO_STORGE_SERVER;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    /*
    ** The IoInterface is what is used to communicate with the Storage Server.
     */
    private final IoInterface storageServerConnection;

    private final int chunkBytesToEncrypt;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    private final BufferManager storageServerBufferManager;
    private final BufferManagerPointer addBufferPointer;
    private BufferManagerPointer writePointer;

    private boolean headerNotBuilt;

    public BuildHeaderToStorageServer(final RequestContext requestContext, final IoInterface storageServerConnection,
                                      final BufferManager storageServerBufferManager, final BufferManagerPointer addBufferPtr,
                                      final int chunkBytesToEncrypt) {

        this.requestContext = requestContext;
        this.storageServerConnection = storageServerConnection;

        this.storageServerBufferManager = storageServerBufferManager;
        this.addBufferPointer = addBufferPtr;

        this.chunkBytesToEncrypt = chunkBytesToEncrypt;

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        headerNotBuilt = true;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
     */
    public BufferManagerPointer initialize() {
        writePointer = storageServerBufferManager.register(this, addBufferPointer);
        return writePointer;
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
        if (headerNotBuilt) {
            /*
             ** Add a buffer if this is the first time through
             */
            storageServerBufferManager.updateProducerWritePointer(addBufferPointer);

            /*
             ** Build the HTTP Header and the Object to be sent
             */
            ByteBuffer msgHdr = storageServerBufferManager.peek(writePointer);
            if (msgHdr != null) {

                String tmp;
                tmp = buildRequestString(chunkBytesToEncrypt);

                str_to_bb(msgHdr, tmp);

                /*
                 ** Need to flip() the buffer so that the limit() is set to the end of where the HTTP Request is
                 **   and the position() reset to 0.
                 */
                msgHdr.flip();

                /*
                 ** Data is now present in the ByteBuffer so the writeInfillPtr needs to be updated,
                 */
                storageServerBufferManager.updateProducerWritePointer(writePointer);
            } else {
                LOG.info("BuildHeaderToStorageServer no buffers");
            }

            headerNotBuilt = false;
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

    private String buildRequestString(final int bytesInContent) {
        return new String("PUT /n/faketenantname" + "" +
                "/b/bucket-5e1910d0-ea13-11e9-851d-234132e0fb02" +
                "/v/StorageServer" +
                "/o/5e223890-ea13-11e9-851d-234132e0fb02  HTTP/1.1\n" +
                "Host: StorageServerWrite\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-Length: " + bytesInContent + "\n\n");
    }

    private void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(CharBuffer.wrap(in), out, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
