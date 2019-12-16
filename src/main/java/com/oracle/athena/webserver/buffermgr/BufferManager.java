package com.oracle.athena.webserver.buffermgr;

import com.oracle.athena.webserver.connectionstate.BufferState;
import com.oracle.athena.webserver.operations.Operation;

public class BufferManager {

    private BufferState[] bufferStateArray;
    private final int bufferArraySize;

    BufferManager(final int bufferCount) {
        this.bufferArraySize = bufferCount;

        bufferStateArray = new BufferState[this.bufferArraySize];
    }

    BufferManagerPointer register(final Operation operation) {
        BufferManagerPointer pointer = new BufferManagerPointer(operation, bufferArraySize);

        return pointer;
    }

    BufferManagerPointer register(final Operation operation, final BufferManagerPointer dependsOn) {
        BufferManagerPointer pointer = new BufferManagerPointer(operation, dependsOn, bufferArraySize);
        dependsOn.addDependsOn(operation);

        return pointer;
    }

    void unregister(final BufferManagerPointer pointer) {
        pointer.terminate();
    }

    /*
    ** This is used to allocate BufferState to the BufferManager.
     */
    void offer(final BufferManagerPointer pointer, final BufferState bufferState) {
    }

    /*
    ** This updates where valid data for the producer is available.
    **
    ** This returns the next location to write data into.
     */
    int updateProducerWritePointer(final BufferManagerPointer pointer) {
        return pointer.updateWriteIndex()
    }

    /*
    ** This returns a BufferState if the readIndex for the consumer is not the
    **   same as the writeIndex for the producer (who the consumer is dependent
    **   upon).
    ** It will update the readIndex if there is a BufferState ready
     */
    BufferState poll(final BufferManagerPointer pointer) {
        if (int readIndex = pointer.getReadIndex(true) != -1) {
            return bufferStateArray[readIndex];
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
    BufferState peek(final BufferManagerPointer pointer) {
        if (int readIndex = pointer.getReadIndex(false) != -1) {
            return bufferStateArray[readIndex];
        }

        return null;
    }

    BufferManagerPointer bookmark(final BufferManagerPointer pointer) {

    }

    /*
    ** This is used to convert a single BufferState into two BufferState and can be used
    **   when a BufferState crosses an HTTP Parse or a Chunk boundary.
     */
    void split(final BufferManagerPointer pointer, final int splitIndex) {

    }
}
