package com.webutils.objectserver.operations;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.operations.StorageServerResponseBufferMetering;
import com.webutils.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class DecryptBuffer implements Operation {

    private static final Logger LOG = LoggerFactory.getLogger(DecryptBuffer.class);

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    public final OperationTypeEnum operationType = OperationTypeEnum.DECRYPT_BUFFER;

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
     ** The DecryptBuffer operation is a consumer of ByteBuffers that are filled in by the SetupChunkRead operation.
     **   The ByteBuffers are placed into BufferManager that is owned by the chunk read operation and it is
     **   registered with this on a per chunk basis.
     ** The buffers are decrypted and placed in the clientWriteBufferManager to be written out to the client.
     **
     ** The encryptedBufferPointer is used to track ByteBuffer(s) that are filled with client object data and are
     **   ready to be decrypted prior to being returned to the client.
     ** The encryptedBufferPointer tracks the inputBufferManager where data is placed following reads from
     **   the Storage Server SocketChannel.
     */
    private final BufferManager inputBufferManager;
    private final BufferManagerPointer encryptedBufferPointer;
    private final Operation encryptedBufferMetering;
    private BufferManagerPointer dataToDecryptPointer;

    /*
    ** The decryptedBufferPointer is where the decrypted data is placed in the writeBufferManager prior to being
    **   transferred to the client.
    ** The decryptedBufferMetering is used to obtain buffers to write the decrypted data into from the
    **   decryptBufferManager. The decryptBufferPointer depends upon the metering Pointer.
     */
    private final BufferManager decryptBufferManager;
    private BufferManagerPointer decryptedBufferPointer;
    private final Operation decryptedBufferMetering;
    private final BufferManagerPointer meteringPointer;

    private final Operation completeCallback;

    /*
     ** The following are used to keep track of how much has been written to this Storage Server and
     **   how much is supposed to be written.
     */
    private int bytesToDecrypt;
    private int bytesDecrypted;

    private int savedSrcPosition;

    /*
     ** This is set when the number of bytes encrypted matches the value passed in through the
     **   HTTP Header, content-length.
     */
    private final AtomicBoolean buffersAllDecrypted;

    /*
    ** The following are the inputs to the decrypt operation:
    **   Operation            - encryptedBufferMetering
    **   BufferManager        - inputBufferMgr
    **   BufferManagerPointer - encryptedBufferPtr (where the encrypted data is being placed)
    **
    **   Operation            - decryptedBufferMetering
    **   BufferManager        - decryptBufferMgr
    **   BufferManagerPointer - meteringPtr
    **
    **   int                  - bytesToDecrypt
     */
    public DecryptBuffer(final RequestContext requestContext, final Operation encryptedBufferMetering,
                         final BufferManager inputBufferMgr, final BufferManagerPointer encryptedBufferPtr,
                         final Operation decryptedBufferMetering, final BufferManager decryptBufferMgr,
                         final BufferManagerPointer meteringPtr, final int bytesToDecrypt, final Operation completeCb) {

        this.requestContext = requestContext;

        this.encryptedBufferMetering = encryptedBufferMetering;
        this.inputBufferManager = inputBufferMgr;
        this.encryptedBufferPointer = encryptedBufferPtr;

        this.decryptedBufferMetering = decryptedBufferMetering;
        this.decryptBufferManager = decryptBufferMgr;
        this.meteringPointer = meteringPtr;

        this.bytesToDecrypt = bytesToDecrypt;

        this.completeCallback = completeCb;

        /*
         ** This starts out not being on any queue
         */
        onExecutionQueue = false;

        buffersAllDecrypted = new AtomicBoolean(false);
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
        ** The decryptedBufferPointer is where data that has been decrypted is placed. This will be the pointer
        **   that is used to write the data back out to the client.
         */
        decryptedBufferPointer = decryptBufferManager.register(this, meteringPointer);

        dataToDecryptPointer = inputBufferManager.register(this, encryptedBufferPointer);

        /*
         ** This keeps track of the number of bytes that have been decrypted.
         */
        bytesDecrypted = 0;

        /*
         ** savedSrcPosition is used to handle the case where the bytes to decrypt starts somewhere besides the start
         **   of the ByteBuffer.
         */
        ByteBuffer readBuffer;
        if ((readBuffer = inputBufferManager.peek(dataToDecryptPointer)) != null) {
            savedSrcPosition = readBuffer.position();

            /*
            ** Add this to the execute queue since there is already data in a buffer to decrypt
             */
            event();
        } else {
            savedSrcPosition = 0;

            /*
            ** There are no buffers waiting to be decrypted so meter out a buffer to force a read
             */
            encryptedBufferMetering.event();
        }

        LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] initialize done savedSrcPosition: " + savedSrcPosition);

        return decryptedBufferPointer;
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
        if (!buffersAllDecrypted.get()) {
            ByteBuffer readBuffer;
            ByteBuffer decryptedBuffer;
            boolean outOfBuffers = false;

            while (!outOfBuffers) {
                if ((readBuffer = inputBufferManager.peek(dataToDecryptPointer)) != null) {
                    /*
                     ** Create a temporary ByteBuffer to hold the readBuffer so that it is not
                     **  affecting the position() and limit() indexes.
                     **
                     ** NOTE: savedSrcPosition will be reset in the encryptBuffer() method
                     */
                    ByteBuffer srcBuffer = readBuffer.duplicate();
                    srcBuffer.position(savedSrcPosition);

                    /*
                     ** Is there an available buffer in the client writeBufferMgr to save the decrypted data
                     */
                    if ((decryptedBuffer = decryptBufferManager.peek(decryptedBufferPointer)) != null) {

                        /*
                         ** Decrypt the buffers and place them into the decryptBufferManager. This is what will
                         */
                        decryptBuffer(srcBuffer, decryptedBuffer);
                    } else {
                        /*
                        ** Need to obtain another buffer to place the decrypted data inot
                         */
                        decryptedBufferMetering.event();
                        outOfBuffers = true;
                    }
                } else {

                    if (bytesDecrypted < bytesToDecrypt) {
                        /*
                        ** Need to read in another buffers worth of encrypted data to feed into the decryption engine
                         */
                        LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] bytesDecrypted: " +
                                bytesDecrypted + " bytesDecrypted: " + bytesDecrypted);

                        encryptedBufferMetering.event();
                    } else if (bytesDecrypted == bytesToDecrypt) {
                        /*
                         ** No more buffers should arrive at this point from the client
                         */
                        LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] all buffers decrypted bytesDecrypted: " +
                                bytesDecrypted + " bytesToDecrypt: " + bytesToDecrypt);

                        buffersAllDecrypted.set(true);
                    }

                    outOfBuffers = true;
                }
            }
        } else {
            /*
             ** Now need to inform the higher level Operation that the decryption has completed
             */
            if (completeCallback != null) {
                completeCallback.complete();
            }
        }
    }

    /*
     */
    public void complete() {

        LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] complete()");

        /*
         ** Remove the reference to the passed in meteringPointer (it is not owned by this Operation)
         */
        decryptBufferManager.unregister(decryptedBufferPointer);
        decryptedBufferPointer = null;

        inputBufferManager.unregister(dataToDecryptPointer);
        dataToDecryptPointer = null;
    }

    public boolean getBuffersAllDecrypted() { return buffersAllDecrypted.get(); }

    /*
     ** The following are used to add the Operation to the event thread's event queue. To simplify the design an
     **   Operation can be added to the immediate execution queue or the delayed execution queue. An Operation
     **   cannot be on the delayed queue sometimes and on the work queue other times. Basically, an Operation is
     **   either designed to perform work as quickly as possible or wait a period of time and try again.
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
        //LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(" + delayedExecutionQueue + ")");
        if (delayedExecutionQueue) {
            LOG.warn("DecryptBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(true) not supposed to be on delayed queue");
        } else if (onExecutionQueue){
            onExecutionQueue = false;
        } else {
            LOG.warn("DecryptBuffer[" + requestContext.getRequestId() + "] markRemovedFromQueue(false) not on a queue");
        }
    }

    public void markAddedToQueue(final boolean delayedExecutionQueue) {
        if (delayedExecutionQueue) {
            LOG.warn("DecryptBuffer[" + requestContext.getRequestId() + "] markAddToQueue(true) not supposed to be on delayed queue");
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
        LOG.warn("DecryptBuffer[" + requestContext.getRequestId() +
                "] hasWaitTimeElapsed() not supposed to be on delayed queue");
        return true;
    }

    /*
     ** This method is used to decrypt a buffer and place it into another location. This method has the following
     **   side effects:
     **     -> If the entire srcBuffer is consumed, it will increment the clientReadBufferMgr.peek(clientBufferPtr)
     **     -> If the entire tgtBuffer is filled, it will increment the
     **          storageServerWriteBufferMgr.updateProducerWritePointer(storageServerWritePtr)
     **
     ** The loop that controls the buffers passed to this method simply grabs the buffer that is being pointed to,
     **   but does not modify the pointer. This method is what actually modifies the pointer to allow partial
     **   buffers to be used multiple times.
     */
    private void decryptBuffer(ByteBuffer srcBuffer, ByteBuffer tgtBuffer) {
        int bytesToDecrypt = srcBuffer.remaining();
        int bytesInTgtBuffer = tgtBuffer.remaining();

        /*
         ** Keeping these LOG statements around in case there is a problem later
         */
        LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] src position: " + srcBuffer.position() +
                " remaining: " + srcBuffer.remaining() + " limit: " + srcBuffer.limit());
        LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] tgt position: " + tgtBuffer.position() +
                " remaining: " + tgtBuffer.remaining() + " limit: " + tgtBuffer.limit());

        /*
         ** The easiest case is when the tgtBuffer can hold all of the bytes in the srcBuffer
         */
        if (bytesToDecrypt <= bytesInTgtBuffer) {
            tgtBuffer.put(srcBuffer);

            LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] 1 - remaining: " + tgtBuffer.remaining());

            /*
             ** This is the case where the amount of data remaining to be encrypted in the srcBuffer
             **   completely fills the tgtBuffer.
             */
            if ((tgtBuffer.remaining() == 0) || ((tgtBuffer.remaining() + bytesDecrypted) == bytesToDecrypt)) {

                /*
                 ** Update the number of bytes that have been encrypted
                 */
                bytesDecrypted += tgtBuffer.limit();
                LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] 1 - bytesToDecrypt: " + bytesToDecrypt +
                        " bytesDecrypted: " + bytesDecrypted);

                /*
                 ** Since the target buffer has been written to, its position() is set to its limit(), so
                 **   reset the position() back to the start.
                 */
                tgtBuffer.flip();
                decryptBufferManager.updateProducerWritePointer(decryptedBufferPointer);
            }

            /*
             ** The tgtBuffer is not full so it needs data from another srcBuffer. Update the read position
             **   for the BufferManager so the next time through the loop, a new readBuffer will be
             **   obtained.
             */
            savedSrcPosition = 0;
            inputBufferManager.updateConsumerReadPointer(dataToDecryptPointer);

        } else {
            LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] full src: " + srcBuffer.remaining() +
                    " tgt: " + tgtBuffer.remaining());

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
             ** Increment the encrypted bytes since this tgt buffer is full
             */
            bytesDecrypted += tgtBuffer.limit();
            LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] 2 - bytesToDecrypt: " + bytesToDecrypt +
                    " bytesDecrypted: " + bytesDecrypted);

            /*
             ** The tgtBuffer is now full.
             */
            tgtBuffer.flip();
            decryptBufferManager.updateProducerWritePointer(decryptedBufferPointer);
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
    public void testDecryption() {
        int[][] allocations = {
                {1024, 2048},
                {1024, 1536},
                {1024, 512},
                {512, 1024},
                {2048, 512},
        };

        final int BUFFERS_TO_ALLOCATE = 10;

        LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] testEncryption() start");

        MemoryManager memoryManager = new MemoryManager(requestContext.getWebServerFlavor());

        /*
         ** Setup the BufferReadMetering to populate the clientReadBufferManager with ByteBuffer(s)
         */
        StorageServerResponseBufferMetering metering = new StorageServerResponseBufferMetering(requestContext,
                memoryManager, inputBufferManager, BUFFERS_TO_ALLOCATE);
        BufferManagerPointer meteringPtr = metering.initialize();

        /*
         ** Create two BufferManagerPointers to add buffers to the two BufferManagers that will
         **   be used to perform the encryption. The buffers added to the clientReadBufferMgr are
         **   initialized to a known pattern.
         */
        BufferManagerPointer readFillPtr = inputBufferManager.register(this, meteringPtr);

        /*
         ** Create the two dependent pointers to read the ByteBuffers from one BufferManager, encrypt the buffer, and
         **   then add it to the StorageServerWriteBufferManager.
         **
         ** NOTE: This needs to be done prior to adding buffers to the BufferManager as the dependent
         **   BufferManagerPointer picks up the producers current write index as its starting read index.
         */
        dataToDecryptPointer = inputBufferManager.register(this, readFillPtr);

        /*
         ** Now create one more dependent BufferManagerPointer on the storageServerWritePointer to read all of
         **   the encrypted data back to insure it matches what is expected.
         */
        BufferManagerPointer validatePtr = decryptBufferManager.register(this, decryptedBufferPointer);

        /*
         ** Now add buffers the the two BufferManagers
         */
        ByteBuffer buffer;
        int fillValue = 0;
        for (int i = 0; i < allocations.length; i++) {
            int capacity = allocations[i][0];

            metering.execute();
            buffer = inputBufferManager.poll(readFillPtr);
            if (buffer != null) {
                buffer.limit(capacity);

                for (int j = 0; j < capacity; j = j + 4) {
                    buffer.putInt(fillValue);
                    fillValue++;
                }

                buffer.flip();
            }

            /*
             ** Now add in the buffers to decrypt the data into
             */
            decryptedBufferMetering.execute();
            capacity = allocations[i][1];
            buffer = decryptBufferManager.poll(decryptedBufferPointer);
            if (buffer != null) {
                buffer.limit(capacity);
            }
        }

        decryptBufferManager.reset(decryptedBufferPointer);

        /*
         ** Need to set the chunkBytesToEncrypt to prevent the encryption loop from doing odd things
         */
        bytesToDecrypt = 0;
        for (int i = 0; i < allocations.length; i++) {
            bytesToDecrypt += allocations[i][0];
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
        int decryptedValue;
        int tgtBuffer = 0;
        fillValue = 0;
        while ((readBuffer = decryptBufferManager.poll(validatePtr)) != null) {
            for (int j = 0; j < readBuffer.limit(); j = j + 4) {
                decryptedValue = readBuffer.getInt();
                if (decryptedValue != fillValue) {
                    System.out.println("Mismatch at targetBuffer: " + tgtBuffer + " index: " + j);
                    break;
                }

                fillValue++;
            }

            tgtBuffer++;
        }

        LOG.info("DecryptBuffer[" + requestContext.getRequestId() + "] compare complete buffers: " + tgtBuffer);

        /*
         ** Cleanup the test. Start by removing all the BufferManagerPointer(s) from the BufferManager(s)
         */
        decryptBufferManager.unregister(validatePtr);
        complete();

        inputBufferManager.unregister(readFillPtr);
        metering.complete();
    }

    /*
     ** Display what this has created and any BufferManager(s) and BufferManagerPointer(s)
     */
    public void dumpCreatedOperations(final int level) {
        LOG.info(" " + level + ":    requestId[" + requestContext.getRequestId() + "] type: " + operationType);

        if (decryptedBufferPointer != null) {
            decryptedBufferPointer.dumpPointerInfo();
        }
        LOG.info("");
    }


}
