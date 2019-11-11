package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.memory.MemoryManager;

import java.nio.ByteBuffer;

/*
 ** This allow the BufferState objects to be managed through a pool to prevent object
 **   creation and garbage collection.
 */
public class BufferStatePool {

    private int allocatedObjects;

    private MemoryManager memoryAllocator;

    public BufferStatePool(final int reqBufferStates, MemoryManager allocator) {

        allocatedObjects = reqBufferStates;
        memoryAllocator = allocator;
    }

    public BufferState allocBufferState(final ConnectionState connState, final BufferStateEnum initialState, final int bufferSize) {
        BufferState bufferState = new BufferState(connState);

        if (bufferState != null) {
            MemoryAvailableCallback memAvailCb;

            memAvailCb = new MemoryAvailableCallback(connState, 1);
            ByteBuffer buffer = memoryAllocator.poolMemAlloc(bufferSize, memAvailCb);
            if (buffer != null) {
                bufferState.assignBuffer(buffer, initialState);
                return bufferState;
            }

            // Need to release the bufferState
        }

        // Unable to allocate the memory
        return null;
    }

    public void freeBufferState(BufferState bufferState) {

        ByteBuffer buffer = bufferState.getBuffer();
        memoryAllocator.poolMemFree(buffer);

        bufferState.assignBuffer(null, BufferStateEnum.INVALID_STATE);
    }

}
