package com.oracle.athena.webserver.operations;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.athena.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;

public class EncryptBuffer implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(EncryptBuffer.class);

    private final int NUM_STORAGE_SERVER_WRITE_BUFFERS = 10;

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.ENCRYPT_BUFFER;

    /*
     ** The RequestContext is used to keep the overall state and various data used to track this Request.
     */
    private final RequestContext requestContext;

    private final MemoryManager memoryManager;

    /*
     ** The following are used to insure that an Operation is never on more than one queue and that
     **   if there is a choice between being on the timed wait queue (onDelayedQueue) or the normal
     **   execution queue (onExecutionQueue) is will always go on the execution queue.
     */
    private boolean onDelayedQueue;
    private boolean onExecutionQueue;
    private long nextExecuteTime;

    /*
    ** The EncryptBuffer operation is a consumer of ByteBuffers that are filled in response to the ReadBuffer
    **   operation. This operation encrypts those buffers and places them into the storageServerWriteBufferManager
    **   as a producer.
     */
    private final BufferManager clientReadBufferMgr;
    private final BufferManager storageServerWriteBufferMgr;

    /*
    ** The encyptInputPointer is used to track ByteBuffer(s) that are filled with client object data and are
    **   ready to be encrypted prior to being written to the Storage Servers.
    ** The encyptInputPointer tracks the clientReadBufferManager where data is placed following reads from
    **   the client connection's SocketChannel.
     */
    private final BufferManagerPointer clientReadPointer;
    private BufferManagerPointer encryptInputPointer;

    private final int chunkSize;

    private final Operation readBufferMetering;

    private final Operation completeCallback;

    private BufferManagerPointer storageServerAddPointer;
    private BufferManagerPointer storageServerWritePtr;

    /*
    ** The following are used to keep track of how much has been written to this Storage Server and
    **   how much is supposed to be written.
     */
    private int chunkBytesToEncrypt;
    private int chunkBytesEncrypted;

    private int savedSrcPosition;

    /*
    ** This is set when the number of bytes encrypted matches the value passed in through the
    **   HTTP Header, content-length.
     */
    private boolean buffersAllEncrypted;

    private ServerIdentifier chunkId;

    /*
     ** SetupChunkWrite is called at the beginning of each chunk (128MB) block of data. This is what sets
     **   up the calls to obtain the VON information and the meta-data write to the database.
     */
    public EncryptBuffer(final RequestContext requestContext, final MemoryManager memoryManager,
                         final BufferManagerPointer readPtr,
                         final Operation completeCb) {

        this.requestContext = requestContext;
        this.memoryManager = memoryManager;
        this.clientReadBufferMgr = this.requestContext.getClientReadBufferManager();
        this.completeCallback = completeCb;

        this.storageServerWriteBufferMgr = this.requestContext.getStorageServerWriteBufferManager();

        this.clientReadPointer = readPtr;

        this.readBufferMetering = requestContext.getOperation(OperationTypeEnum.METER_READ_BUFFERS);

        /*
         ** This starts out not being on any queue
         */
        onDelayedQueue = false;
        onExecutionQueue = false;
        nextExecuteTime = 0;

        chunkSize = this.requestContext.getChunkSize();

        buffersAllEncrypted = false;
    }

    public OperationTypeEnum getOperationType() {
        return operationType;
    }

    /*
     ** This returns the BufferManagerPointer obtained by this operation, if there is one. If this operation
     **   does not use a BufferManagerPointer, it will return null.
     */
    public BufferManagerPointer initialize() {
        encryptInputPointer = clientReadBufferMgr.register(this, clientReadPointer);

        /*
         ** This keeps track of the number of bytes that have been encrypted. When it reaches a chunk
         **   boundary, it then starts off a new chunk write sequence.
         */
        chunkBytesEncrypted = 0;

        chunkBytesToEncrypt = requestContext.getRequestContentLength();

        /*
        ** savedSrcPosition is used to handle the case where there are no buffers available to place
        **   encrypted data into, so this operation will need to wait until buffers are available.
         */
        ByteBuffer readBuffer;
        if ((readBuffer = clientReadBufferMgr.peek(encryptInputPointer)) != null) {
            savedSrcPosition = readBuffer.position();
        } else {
            savedSrcPosition = 0;
        }

        /*
         ** The storageServerWritePtr is a producer of ByteBuffers in the storage server write BufferManager.
         **   The buffers produced are used by the WriteToStorageServer operation(s) to stream data out to
         **   the storage servers.
         */
        storageServerAddPointer = storageServerWriteBufferMgr.register(this);
        storageServerWriteBufferMgr.bookmark(storageServerAddPointer);

        for (int i = 0; i < NUM_STORAGE_SERVER_WRITE_BUFFERS; i++) {
            ByteBuffer writeBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, storageServerWriteBufferMgr);
            storageServerWriteBufferMgr.offer(storageServerAddPointer, writeBuffer);
        }

        storageServerWritePtr = storageServerWriteBufferMgr.register(this, storageServerAddPointer);

        LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] initialize done");

        return encryptInputPointer;
    }

    /*
    ** The EncryptBuffer operation will have its "event()" method invoked whenever there is data read into
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
        if (!buffersAllEncrypted) {
            ByteBuffer readBuffer;
            ByteBuffer encryptedBuffer;
            boolean outOfBuffers = false;

            while (!outOfBuffers) {
                if ((readBuffer = clientReadBufferMgr.peek(encryptInputPointer)) != null) {
                    /*
                     ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
                     **  affecting the position() and limit() indexes
                     */
                    ByteBuffer srcBuffer = readBuffer.duplicate();
                    srcBuffer.position(savedSrcPosition);

                    /*
                     ** Is there an available buffer in the storageServerWriteBufferMgr
                     */
                    if ((encryptedBuffer = storageServerWriteBufferMgr.peek(storageServerWritePtr)) != null) {

                        /*
                         ** Encrypt the buffers and place them into the storageServerWriteBufferMgr
                         */
                        encryptBuffer(srcBuffer, encryptedBuffer);
                    } else {
                        outOfBuffers = true;
                    }
                } else {

                    if (chunkBytesEncrypted < chunkBytesToEncrypt) {
                        readBufferMetering.event();
                    } else if (chunkBytesEncrypted == chunkBytesToEncrypt) {
                        /*
                        ** No more buffers should arrive at this point from the client
                         */
                        LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] all buffers encrypted chunkBytesEncrypted: " +
                                chunkBytesEncrypted + " chunkBytesEncrypted: " + chunkBytesEncrypted);

                        buffersAllEncrypted = true;
                    }

                    outOfBuffers = true;
                }
            }
        } else {
            /*
            ** Need to cleanup here from the Encrypt operation
             */
            if (requestContext.hasStorageServerResponseArrived(chunkId)) {
                int result = requestContext.getStorageResponseResult(chunkId);
                LOG.info("ChunkWriteComplete result: " + result);

                complete();
            } else {
                LOG.warn("ChunkWriteComplete waiting for result");
            }

            chunkId = null;
        }
    }

    /*
     ** This will never be called for the CloseOutRequest. When the execute() method completes, the
     **   RequestContext is no longer "running".
     */
    public void complete() {

        LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] complete()");

        /*
        ** Remove the reference to the passed in encryptInputPointer (it is not owned by this Operation)
         */
        encryptInputPointer = null;

        storageServerWriteBufferMgr.unregister(storageServerWritePtr);
        storageServerWritePtr = null;

        storageServerWriteBufferMgr.reset(storageServerAddPointer);
        for (int i = 0; i < NUM_STORAGE_SERVER_WRITE_BUFFERS; i++) {
            ByteBuffer buffer = storageServerWriteBufferMgr.getAndRemove(storageServerAddPointer);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer);
            }
        }
        storageServerWriteBufferMgr.unregister(storageServerAddPointer);
        storageServerAddPointer = null;

        /*
        ** Now need to send out the final status
         */
        if (completeCallback != null){
            completeCallback.complete();
        }
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
        //LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (onDelayedQueue) {
            if (!delayedExecutionQueue) {
                LOG.warn("EncryptBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on delayed queue");
            }

            onDelayedQueue = false;
            nextExecuteTime = 0;
        } else if (onExecutionQueue){
            if (delayedExecutionQueue) {
                LOG.warn("EncryptBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not supposed to be on workQueue");
            }

            onExecutionQueue = false;
        } else {
            LOG.warn("EncryptBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ") not on a queue");
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

        //LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] waitTimeElapsed " + currTime);
        return true;
    }

    /*
    ** This method is used to encrypt a buffer and place it into another location. This method has the following
    **   side effects:
    **     -> If the entire srcBuffer is consumed, it will increment the clientReadBufferMgr.peek(clientBufferPtr)
    **     -> If the entire tgtBuffer is filled, it will increment the
    **          storageServerWriteBufferMgr.updateProducerWritePointer(storageServerWritePtr)
    **
    ** The loop that controls the buffers passed to this method simply grabs the buffer that is being pointed to,
    **   but does not modify the pointer. This method is what actually modifies the pointer to allow partial
    **   buffers to be used multiple times.
     */
    private void encryptBuffer(ByteBuffer srcBuffer, ByteBuffer tgtBuffer) {
        int bytesToEncrypt = srcBuffer.remaining();
        int bytesInTgtBuffer = tgtBuffer.remaining();

        /*
        ** Keeping these LOG statements around in case there is a problem later
        **
        LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] src position: " + srcBuffer.position() +
                " remaining: " + srcBuffer.remaining() + " limit: " + srcBuffer.limit());
        LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] tgt position: " + tgtBuffer.position() +
                " remaining: " + tgtBuffer.remaining() + " limit: " + tgtBuffer.limit());
         */

        /*
        ** The easiest case is when the tgtBuffer can hold all of the bytes in the srcBuffer
         */
        if (bytesToEncrypt <= bytesInTgtBuffer) {
            tgtBuffer.put(srcBuffer);

            //LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] 1 - remaining: " + tgtBuffer.remaining());

            /*
            ** This is the case where the amount of data remaining to be encrypted in the srcBuffer
            **   completely fills the tgtBuffer.
             */
            if ((tgtBuffer.remaining() == 0) || ((tgtBuffer.remaining() + chunkBytesEncrypted) == chunkBytesToEncrypt)) {

                /*
                ** The buffer is either full or all the data has been received for the client object
                 */
                checkForNewChunkStart();

                /*
                ** Update the number of bytes that have been encrypted
                 */
                chunkBytesEncrypted += tgtBuffer.limit();
                //LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] 1 - chunkBytesToEncrypt: " + chunkBytesToEncrypt +
                //        " chunkBytesEncrypted: " + chunkBytesEncrypted);

                /*
                ** Since the target buffer has been written to, its position() is set to its limit(), so
                **   reset the position() back to the start.
                 */
                tgtBuffer.flip();
                storageServerWriteBufferMgr.updateProducerWritePointer(storageServerWritePtr);
            }

            /*
            ** The tgtBuffer is not full so it needs data from another srcBuffer. Update the read position
            **   for the BufferManager so the next time through the loop, a new readBuffer will be
            **   obtained.
             */
            savedSrcPosition = 0;
            clientReadBufferMgr.updateConsumerReadPointer(encryptInputPointer);

        } else {
            //LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] full src: " + srcBuffer.remaining() +
            //        " tgt: " + tgtBuffer.remaining());

            /*
             ** This is the case where the srcBuffer has more data to encrypt than the tgtBuffer can accept.
             */
            tgtBuffer.put(srcBuffer.array(), srcBuffer.position(), bytesInTgtBuffer);

            /*
            ** The call to put() which uses a starting position and a count does not update the position()
            **   for the source of the data.
             */
            savedSrcPosition = srcBuffer.position() + bytesInTgtBuffer;

            /*
            ** The target buffer is full, so check for a chunk start
             */
            checkForNewChunkStart();

            /*
            ** Increment the encrypted bytes since this tgt buffer is full
             */
            chunkBytesEncrypted += tgtBuffer.limit();
            //LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] 2 - chunkBytesToEncrypt: " + chunkBytesToEncrypt +
            //        " chunkBytesEncrypted: " + chunkBytesEncrypted);

            /*
            ** The tgtBuffer is now full.
             */
            tgtBuffer.flip();
            storageServerWriteBufferMgr.updateProducerWritePointer(storageServerWritePtr);
        }
    }

    /*
     ** If this buffer is the first one for a chunk, add a bookmark and also kick off the
     **   SetupWriteChunk operation
     */
    private void checkForNewChunkStart() {
        /*
         ** The buffer is full, so check if it is time to start a new chunk
         */
        if ((chunkBytesEncrypted % chunkSize) == 0) {
            /*
             ** This bookmark will be used by the WriteToStorageServer operations. The WriteStorageServer
             **   operations will be created by the SetupChunkWrite once it has determined the
             **   VON information.
             */
            storageServerWriteBufferMgr.bookmarkThis(storageServerWritePtr);

            /*
             ** Now create the SetupChunkWrite and start it running
             */
            chunkId = new ServerIdentifier(InetAddress.getLoopbackAddress(),
                    RequestContext.STORAGE_SERVER_PORT_BASE, 0);
            SetupChunkWrite setupChunkWrite = new SetupChunkWrite(requestContext, chunkId,
                    memoryManager, storageServerWritePtr, chunkBytesToEncrypt, this);
            setupChunkWrite.initialize();
            setupChunkWrite.event();
        }
    }


    /*
    ** This is designed to test the boundary cases where the following happens:
    **
    **   -> tgtBuffer is larger than the srcBuffer
    **   -> tgtBuffer is filled at the same time as the srcBuffer is drained
    **   -> tgtBuffer is smaller than the srcBuffer so it takes multiple tgtBuffers to fill the tgtBuffer
    **
    ** The allocation patterns in the two BufferManger(s) are:
    **   ClientReadBufferMgr         - 1k, 1k, 1k, 0.5k, 2k
    **   StorageServerWriteBufferMgr - 2k    , 1.5k,   , 0.5k, 1k, 0.5k
     */
    public void testEncryption() {
        int[][] allocations = {
                {1024, 2048},
                {1024, 1536},
                {1024, 512},
                {512, 1024},
                {2048, 512},
        };

        LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] testEncryption() start");

        /*
         ** Create two BufferManagerPointers to add buffers to the two BufferManagers that will
         **   be used to perform the encryption. The buffers added to the clientReadBufferMgr are
         **   initialized to a known pattern.
         */
        BufferManagerPointer readFillPtr = clientReadBufferMgr.register(this);
        BufferManagerPointer writeFillPtr = storageServerWriteBufferMgr.register(this);

        /*
         ** Create the two dependent pointers to read the ByteBuffers from one BufferManager, encrypt the buffer, and
         **   then add it to the StorageServerWriteBufferManager.
         **
         ** NOTE: This needs to be done prior to adding buffers to the BufferManager as the dependent
         **   BufferManagerPointer picks up the producers current write index as its starting read index.
         */
        encryptInputPointer = clientReadBufferMgr.register(this, readFillPtr);
        storageServerWritePtr = storageServerWriteBufferMgr.register(this, writeFillPtr);

        /*
         ** Now create one more dependent BufferManagerPointer on the storageServerWritePtr to read all of
         **   the encrypted data back to insure it matches what is expected.
         */
        BufferManagerPointer validatePtr = storageServerWriteBufferMgr.register(this, storageServerWritePtr);

        /*
        ** Now add buffers the the two BufferManagers
         */
        ByteBuffer buffer;
        int fillValue = 0;
        for (int i = 0; i < allocations.length; i++) {
            int capacity = allocations[i][0];
            buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, clientReadBufferMgr);
            buffer.limit(capacity);

            for (int j = 0; j < capacity; j = j + 4) {
                buffer.putInt(fillValue);
                fillValue++;
            }

            buffer.flip();
            clientReadBufferMgr.offer(readFillPtr, buffer);

            /*
             ** Now add in the buffers to encrypt the data into
             */
            capacity = allocations[i][1];
            buffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, storageServerWriteBufferMgr);
            buffer.limit(capacity);

            storageServerWriteBufferMgr.offer(writeFillPtr, buffer);
        }

        /*
        ** Need to set the chunkBytesToEncrypt to prevent the encryption loop from doing odd things
         */
        chunkBytesToEncrypt = 0;
        for (int i = 0; i < allocations.length; i++) {
            chunkBytesToEncrypt += allocations[i][0];
        }

        /*
         ** Run the encryption routine to process all of the buffers
         */
        execute();

        /*
         ** Now read in the buffers from the StorageServerWriteBufferMgr and validate the data within the buffer.
         **   This has the side effect of removing the ByteBuffer(s) from the storageServerWriteBufferMgr
         **   and setting its pointers to null.
         */
        ByteBuffer readBuffer;
        int encryptedValue;
        int tgtBuffer = 0;
        fillValue = 0;
        while ((readBuffer = storageServerWriteBufferMgr.getAndRemove(validatePtr)) != null) {
            for (int j = 0; j < readBuffer.limit(); j = j + 4) {
                encryptedValue = readBuffer.getInt();
                if (encryptedValue != fillValue) {
                    System.out.println("Mismatch at targetBuffer: " + tgtBuffer + " index: " + j);
                    break;
                }

                fillValue++;
            }

            memoryManager.poolMemFree(readBuffer);

            tgtBuffer++;
        }

        LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] compare complete buffers: " + tgtBuffer);

        /*
        ** Cleanup the test. Start by removing all the BufferManagerPointer(s) from the BufferManager(s)
         */
        storageServerWriteBufferMgr.unregister(validatePtr);

        clientReadBufferMgr.unregister(encryptInputPointer);
        storageServerWriteBufferMgr.unregister(storageServerWritePtr);

        /*
        ** Free up the ByteBuffer(s) that were allocated for this test
         */
        clientReadBufferMgr.reset(readFillPtr);
        for (int i = 0; i < allocations.length; i++) {
            buffer = clientReadBufferMgr.getAndRemove(readFillPtr);
            if (buffer != null) {
                memoryManager.poolMemFree(buffer);
            } else {
                LOG.info("EncryptBuffer[" + requestContext.getRequestId() + "] null buffer readFillPtr index: " + readFillPtr.getCurrIndex());
            }
        }

        clientReadBufferMgr.unregister(readFillPtr);
        storageServerWriteBufferMgr.unregister(writeFillPtr);
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);
        if (storageServerAddPointer != null) {
            storageServerAddPointer.dumpPointerInfo();
        }

        if (storageServerWritePtr != null) {
            storageServerWritePtr.dumpPointerInfo();
        }
        LOG.info("");
    }

}
