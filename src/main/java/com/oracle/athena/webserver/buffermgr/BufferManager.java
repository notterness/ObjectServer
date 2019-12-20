package com.oracle.athena.webserver.buffermgr;

import com.oracle.athena.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

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

    private int identifier;

    public BufferManager(final int bufferCount) {
        this.bufferArraySize = bufferCount;

        this.bufferArray = new ByteBuffer[this.bufferArraySize];

        this.identifier = 1;
    }

    public BufferManagerPointer register(final Operation operation) {

        LOG.info("BufferManager register("  + identifier + ":" + operation.getOperationType() + ")");
        BufferManagerPointer pointer = new BufferManagerPointer(operation, bufferArraySize, identifier);
        identifier++;

        return pointer;
    }

    public BufferManagerPointer register(final Operation operation, final BufferManagerPointer dependsOn) {
        LOG.info("BufferManager register(" + identifier + ":" + operation.getOperationType() + ") depends on (" +
                + dependsOn.getIdentifier() + ":" + dependsOn.getOperationType() + ")");

        BufferManagerPointer pointer = new BufferManagerPointer(operation, dependsOn, bufferArraySize, identifier);
        identifier++;
        dependsOn.addDependsOn(operation);

        return pointer;
    }

    public void unregister(final BufferManagerPointer pointer) {
        LOG.info("BufferManager unregister(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ")" );

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

        }

        bufferArray[writeIndex] = buffer;

        /*
        ** Now the pointer can be advanced to allow anything waiting on this to be evented.
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

        //LOG.info("BufferManager updateProducerWritePointer(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") writeIndex: " + writeIndex);

        return writeIndex;
    }

    /*
    ** Update the read index for this consumer
     */
    public int updateConsumerReadPointer(final BufferManagerPointer pointer) {

        int readIndex = pointer.updateReadIndex();
        //LOG.info("BufferManager updateConsumerReadPointer(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") readIndex: " + readIndex);

        return readIndex;
    }

    /*
     ** This returns a BufferState if the readIndex for the consumer is not the
     **   same as the writeIndex for the producer (who the consumer is dependent
     **   upon).
     ** It will update the readIndex if there is a BufferState ready
     */
    public ByteBuffer poll(final BufferManagerPointer pointer) {
        int readIndex = pointer.getReadIndex(true);

        //LOG.info("BufferManager poll(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") readIndex: " + readIndex);

        if (readIndex != -1) {
            return bufferArray[readIndex];
        }

        return null;
    }

    /*
     ** This returns a BufferState if the readIndex for the consumer is not the
     **   same as the writeIndex for the producer (who the consumer is dependent
     **   upon).
     ** It will update the readIndex if there is a BufferState ready and will remove the reference to
     **   the ByteBuffer from the BufferManager.
     */
    public ByteBuffer getAndRemove(final BufferManagerPointer pointer) {
        int readIndex = pointer.getReadIndex(true);

        //LOG.info("BufferManager poll(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") readIndex: " + readIndex);

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
     ** It will NOT update the readIndex if there is a BufferState ready. This will
     **   allow a consumer to decide if it is done processing the BufferState or it
     **   needs to operate on it again (i.e. the write could not empty the BufferState
     **   so another write attempt needs to be made at a later point in time).
     */
    public ByteBuffer peek(final BufferManagerPointer pointer) {
        int readIndex = pointer.getReadIndex(false);

        //LOG.info("BufferManager peek(" + pointer.getIdentifier() + ":" + pointer.getOperationType() + ") readIndex: " + readIndex);

        if (readIndex != -1) {
            return bufferArray[readIndex];
        }

        return null;
    }

    /*
    ** This is used to add a new pointer to within the BufferManager to allow multiple
    **   streams to consume data from different places within the buffer ring.
     */
    public void bookmark(final BufferManagerPointer pointer) {

        pointer.setBookmark();
    }

    /*
     ** Reset the BufferManager back to its pristine state. That means that there are no
     **   registered BufferManagerPointers or dependencies remaining associated with the
     **   BufferManager.
     */
    public void reset() {

    }


    public int reset(final BufferManagerPointer pointer) {
        return pointer.reset();
    }


    /*
    ** This is used to convert a single BufferState into two BufferState and can be used
    **   when a BufferState crosses an HTTP Parse or a Chunk boundary.
     */
    void split(final BufferManagerPointer pointer, final int splitIndex) {

    }
}
