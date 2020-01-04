package com.oracle.athena.webserver.buffermgr;

import com.oracle.athena.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferManager {

    private static final Logger LOG = LoggerFactory.getLogger(BufferManager.class);

    /*
    ** The BufferManager uses an array of ByteBuffer(s) to implement a ring buffer that can have multiple
    **   producers and multiple consumers. The consumers are tied (dependency) to the producer they are interesting
    **   in. When the producer updates their pointer into the ring, that will cause all of the consumers
    **   to be event(ed) which will allow them to run.
     */
    private ByteBuffer[] bufferArray;
    private final int bufferArraySize;

    private final int bufferManagerIdentifier;
    private final AtomicInteger identifier;

    private final String bufferManagerName;


    public BufferManager(final int bufferCount, final String bufferMgrName, final int identifier) {
        this.bufferArraySize = bufferCount;
        this.bufferArray = new ByteBuffer[this.bufferArraySize];

        this.bufferManagerIdentifier = identifier;
        this.identifier = new AtomicInteger(identifier);

        this.bufferManagerName = bufferMgrName;
    }

    public BufferManagerPointer register(final Operation operation) {

        int currIdentifier = identifier.getAndIncrement();

        LOG.info("register " + bufferManagerName + " ("  + currIdentifier + ":" + operation.getOperationType() + ")");
        BufferManagerPointer pointer = new BufferManagerPointer(operation, bufferArraySize, currIdentifier);

        return pointer;
    }

    public BufferManagerPointer register(final Operation operation, final BufferManagerPointer dependsOn) {
        int currIdentifier = identifier.getAndIncrement();

        if (operation != null) {
            LOG.info("register " + bufferManagerName + " (" + currIdentifier + ":" + operation.getOperationType() +
                    ") depends on (" + dependsOn.getIdentifier() + ":" + dependsOn.getOperationType() + ")");
        } else {
            LOG.info("register " + bufferManagerName + " (" + currIdentifier + ": null) depends on (" +
                    + dependsOn.getIdentifier() + ":" + dependsOn.getOperationType() + ")");
        }

        BufferManagerPointer pointer = new BufferManagerPointer(operation, dependsOn, bufferArraySize, currIdentifier);

        return pointer;
    }

    public BufferManagerPointer register(final Operation operation, final BufferManagerPointer dependsOn, final int maxBytesToConsume) {
        int currIdentifier = identifier.getAndIncrement();

        LOG.info("register " + bufferManagerName + " (" + currIdentifier + ":" + operation.getOperationType() + ") depends on (" +
                + dependsOn.getIdentifier() + ":" + dependsOn.getOperationType() + ") maxBytesToConsume: " +
                maxBytesToConsume);

        BufferManagerPointer pointer = new BufferManagerPointer(operation, dependsOn, bufferArraySize, currIdentifier, maxBytesToConsume);

        return pointer;
    }

    public void unregister(final BufferManagerPointer pointer) {
        LOG.info("unregister " + bufferManagerName + " (" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ")" );

        pointer.terminate();
    }

    /*
    ** This is used to allocate BufferState to the BufferManager. It adds a ByteBuffer and then
    **   increments the pointer for the available buffers.
     */
    public void offer(final BufferManagerPointer pointer, final ByteBuffer buffer) {
        int writeIndex = pointer.getWriteIndex();

        //LOG.info("BufferManager offer(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") writeIndex: " + writeIndex);

        /*
        ** If a ByteBuffer is being added to the BufferManager, make sure that the current
        **   array location does not have one already assigned.
         */
        if (bufferArray[writeIndex] != null) {
            LOG.warn("BufferManager(" + bufferManagerName + ") offer() writeIndex in use writeIndex: " + writeIndex);
        }

        bufferArray[writeIndex] = buffer;

        /*
        ** Now the pointer can be advanced to allow anything waiting on this to be event(ed).
         */
        pointer.updateWriteIndex();
    }

    /*
    ** This updates where valid data for the producer is available.
    **
    ** This returns the next location to write data into.
     */
    public int updateProducerWritePointer(final BufferManagerPointer pointer) {

        int writeIndex = pointer.updateWriteIndex();

        //LOG.info("updateProducerWritePointer(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") writeIndex: " + writeIndex);

        return writeIndex;
    }

    /*
    ** Update the read index for this consumer
     */
    public int updateConsumerReadPointer(final BufferManagerPointer pointer) {

        int readIndex = pointer.updateReadIndex();
        //LOG.info("updateConsumerReadPointer(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") readIndex: " + readIndex);

        return readIndex;
    }

    /*
     ** This returns a ByteBuffer if the readIndex for the consumer is not the
     **   same as the writeIndex for the producer (who the consumer is dependent
     **   upon).
     ** For producers, it will insure that the producer is not wrapping around onto its
     **   dependent consumers. This checks that the producer's writeIndex is at least
     **   one behind the readIndex(es) of its consumers.
     ** It will update the readIndex if there is a ByteBuffer returned.
     */
    public ByteBuffer poll(final BufferManagerPointer pointer) {
        int readIndex = pointer.getReadIndex(true);

        //LOG.info("poll(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") readIndex: " + readIndex);

        if (readIndex != -1) {
            return bufferArray[readIndex];
        }

        return null;
    }

    /*
     ** This returns a ByteBuffer if the readIndex for the consumer is not the
     **   same as the writeIndex for the producer (who the consumer is dependent
     **   upon).
     ** It will update the readIndex if there is a ByteBuffer ready and will remove the reference to
     **   the ByteBuffer from the BufferManager.
     */
    public ByteBuffer getAndRemove(final BufferManagerPointer pointer) {
        int readIndex = pointer.getReadIndex(true);

        //LOG.info("poll(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") readIndex: " + readIndex);

        if (readIndex != -1) {
            ByteBuffer buffer = bufferArray[readIndex];
            bufferArray[readIndex] = null;

            return buffer;
        }

        return null;
    }

    /*
     ** This returns a BufferState if the readIndex for the consumer is not the
     **   same as the writeIndex for the producer (who the consumer is dependent
     **   upon).
     ** For producers, it will insure that the producer is not wrapping around onto its
     **   dependent consumers.
     ** It will NOT update the readIndex if there is a ByteBuffer available. This will
     **   allow a consumer to decide if it is done processing the ByteBuffer or it
     **   needs to operate on it again (i.e. the write could not empty the ByteBuffer
     **   so another write attempt needs to be made at a later point in time).
     */
    public ByteBuffer peek(final BufferManagerPointer pointer) {
        int readIndex = pointer.getReadIndex(false);

        //LOG.info("peek " + bufferManagerName + " (" + pointer.getIdentifier() + ":" + pointer.getOperationType() +
        //        ") readIndex: " + readIndex);

        if (readIndex != -1) {
            return bufferArray[readIndex];
        }

        return null;
    }

    /*
    ** This is used to add a new pointer (really an index) to within the BufferManager to allow multiple
    **   streams to consume data from different places within the buffer ring.
     */
    public void bookmark(final BufferManagerPointer pointer) {

        pointer.setBookmark();
    }

    /*
     ** This is used to add a new pointer (really an index) to within the BufferManager to allow multiple
     **   streams to consume data from different places within the buffer ring.
     */
    public void bookmarkThis(final BufferManagerPointer pointer) {

        pointer.setBookmark(pointer.getCurrIndex());
    }

    /*
     ** Reset the BufferManager back to its pristine state. That means that there are no
     **   registered BufferManagerPointers or dependencies remaining associated with the
     **   BufferManager.
     */
    public void reset() {

        LOG.info("BufferManager(" + bufferManagerName + ") reset()");

        /*
        ** Make sure all the BufferPointer values are null
         */
        for (int i = 0; i < bufferArraySize; i++) {
            if (bufferArray[i] != null) {
                LOG.warn("BufferManager(" + bufferManagerName + ") reset() non null buffer i: " + i);
                bufferArray[i] = null;
            }
        }
    }


    /*
    ** The following sets the BufferManagerPointer bufferIndex back to 0.
     */
    public int reset(final BufferManagerPointer pointer) {
        return pointer.reset();
    }

    public void dumpInformation() {
        LOG.info("BufferManager " + bufferManagerName + " id: " + bufferManagerIdentifier);
    }

}
