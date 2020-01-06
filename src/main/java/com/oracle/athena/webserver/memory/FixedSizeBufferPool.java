package com.oracle.athena.webserver.memory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic class to encapsulate memory management.
 * <p>
 */
public class FixedSizeBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(FixedSizeBufferPool.class);

    final private int bufferSize;
    final private int bufferCount;

    final private BlockingQueue<MemoryTracking> freeQueue;
    final private BlockingQueue<MemoryTracking> inUseQueue;

    public FixedSizeBufferPool( int bufferSize, int bufferCount ) {
        this.bufferSize = bufferSize;
        this.bufferCount = bufferCount;

        freeQueue = new LinkedBlockingQueue<>(bufferCount);
        inUseQueue = new LinkedBlockingQueue<>(bufferCount);
        for (int i = 0; i < bufferCount; ++i) {
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            if (buffer != null) {
                MemoryTracking tracker = new MemoryTracking(buffer, i);

                freeQueue.add(tracker);
            }
        }

    }

    public int getBufferCount() { return bufferCount; }

    public int getUnusedBufferCount() { return freeQueue.size(); }

    public ByteBuffer poolMemAlloc(final BufferManager bufferManager) {
        // Instead of calling allocate, perhaps grab a ByteBuffer chunk out of a larger cache
        // If none available, will return null and queue up callback (if supplied) for next free.
        MemoryTracking tracker = freeQueue.poll();
        if (tracker != null) {
            tracker.setBufferManager(bufferManager);

            inUseQueue.add(tracker);

            return tracker.getBuffer();
        }
        return null;
    }

    public void poolMemFree(final ByteBuffer buffer, final BufferManager bufferManager) {

        MemoryTracking tracker = removeTracker(buffer);
        if (tracker != null) {
            buffer.clear();
            freeQueue.add(tracker);
        } else {
            if (bufferManager != null) {
                LOG.warn("Not on freeQueue " + bufferManager.getBufferManagerName());
            } else {
                LOG.warn("Not on freeQueue null BufferManager");
            }
        }
    }

    private MemoryTracking removeTracker(final ByteBuffer buffer) {
        for (MemoryTracking tracker : inUseQueue) {
            if (tracker.getBuffer().equals(buffer)) {
                inUseQueue.remove(tracker);
                return tracker;
            }
        }

        return null;
    }

    public void dumpInUseQueue() {

        for (MemoryTracking memoryTracking : inUseQueue) {
            LOG.warn("inUse " + memoryTracking.getOwner());
        }
    }
}

