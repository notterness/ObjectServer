package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.memory.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/*
 ** This allow the BufferState objects to be managed through a pool to prevent object
 **   creation and garbage collection.
 */
public class BufferStatePool {

    private static final Logger LOG = LoggerFactory.getLogger(BufferStatePool.class);

    private int allocatedObjects;

    private MemoryManager memoryAllocator;

    private BlockingQueue<BufferState> inUseList;

    public BufferStatePool(final int reqBufferStates, MemoryManager allocator) {

        allocatedObjects = reqBufferStates;
        memoryAllocator = allocator;

        inUseList = new LinkedBlockingQueue<>(allocatedObjects * 2);
    }

    public BufferState allocBufferState(final ConnectionState connState, final BufferStateEnum initialState, final int bufferSize) {
        BufferState bufferState = new BufferState(connState);

        if (bufferState != null) {
            MemoryAvailableCallback memAvailCb;

            memAvailCb = new MemoryAvailableCallback(connState, 1);
            ByteBuffer buffer = memoryAllocator.poolMemAlloc(bufferSize, memAvailCb);
            if (buffer != null) {
                bufferState.assignBuffer(buffer, initialState);

                if (!inUseList.offer(bufferState)) {
                    LOG.error("ConnectionState[" + connState.getConnStateId() + "] Unable to add to inUseList");
                }
                return bufferState;
            }

            // Need to release the bufferState
        }

        // Unable to allocate the memory
        return null;
    }

    public BufferState allocBufferState(final ConnectionState connState, final BufferStateEnum initialState,
                                        final int appBufferSize, final int netBufferSize) {
        BufferState bufferState = new BufferState(connState);

        if (bufferState != null) {
            MemoryAvailableCallback memAvailCb;
            ByteBuffer appBuffer;
            ByteBuffer netBuffer;

            memAvailCb = new MemoryAvailableCallback(connState, 2);
            appBuffer = memoryAllocator.poolMemAlloc(appBufferSize, memAvailCb);
            if (appBuffer != null) {
                netBuffer = memoryAllocator.poolMemAlloc(netBufferSize, memAvailCb);
                if (netBuffer != null) {
                    bufferState.assignBuffer(appBuffer, netBuffer, initialState);

                    if (!inUseList.offer(bufferState)) {
                        LOG.error("ConnectionState[" + connState.getConnStateId() + "] Unable to add to inUseList");
                    }

                    return bufferState;
                }else {
                    /* TODO: May need composition strategy (hold this buffer,
					   wait for the second buffer).
					 */
                    memoryAllocator.poolMemFree(appBuffer);
                }
            }

            // Need to release the bufferState
        }

        // Unable to allocate the memory
        return null;
    }


    public void freeBufferState(BufferState bufferState) {

        ByteBuffer buffer = bufferState.getBuffer();
        memoryAllocator.poolMemFree(buffer);
        buffer = bufferState.getNetBuffer();
        if (buffer != null) {
            memoryAllocator.poolMemFree(buffer);
        }
        bufferState.assignBuffer(null, null, BufferStateEnum.INVALID_STATE);

        inUseList.remove(bufferState);
    }

    public boolean showInUseBufferState(final int threadId) {
        boolean inUseBufferState = false;
        BufferState bufferState;

        LOG.info("ServerWorkerThread(" + threadId + ") showInUseBufferState()");

        Iterator<BufferState> iter = inUseList.iterator();
        while (iter.hasNext()) {
            bufferState = iter.next();
            iter.remove();
            LOG.error("BufferState owned by [" + bufferState.getOwnerId() + "] currState: " + bufferState.getBufferState() +
                    " next connection state: " + bufferState.getConnectionNextState().toString());

            inUseBufferState = true;
        }

        return inUseBufferState;
    }

}
