package com.webutils.webserver.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.operations.OperationTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic class to encapsulate memory management.
 * <p>
 */
public class FixedSizeBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(FixedSizeBufferPool.class);

    final private int bufferCount;

    final private BlockingQueue<MemoryTracking> freeQueue;
    final private BlockingQueue<MemoryTracking> inUseQueue;

    public FixedSizeBufferPool( int bufferSize, int bufferCount ) {
        this.bufferCount = bufferCount;

        boolean allocErrorLogged = false;
        int allocErrors = 0;

        freeQueue = new LinkedBlockingQueue<>(bufferCount);
        inUseQueue = new LinkedBlockingQueue<>(bufferCount);
        for (int i = 0; i < bufferCount; ++i) {
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            if (buffer != null) {
                MemoryTracking tracker = new MemoryTracking(buffer, i);

                freeQueue.add(tracker);
            } else {
                if (!allocErrorLogged) {
                    LOG.error("FixedSizeBufferPool allocation failed");
                    allocErrorLogged = true;
                }

                allocErrors++;
            }
        }

        if (allocErrors != 0) {
            LOG.error("FixedSizeBufferPool allocation failed bufferCount: " + bufferCount + " allocErrors: " + allocErrors);
        }
    }

    public int getBufferCount() { return bufferCount; }

    public int getUnusedBufferCount() { return freeQueue.size(); }

    public ByteBuffer poolMemAlloc(final BufferManager bufferManager, final OperationTypeEnum allocator) {
        // Instead of calling allocate, perhaps grab a ByteBuffer chunk out of a larger cache
        // If none available, will return null and queue up callback (if supplied) for next free.
        MemoryTracking tracker = freeQueue.poll();
        if (tracker != null) {
            tracker.setBufferManager(bufferManager, allocator);

            inUseQueue.add(tracker);

            return tracker.getBuffer();
        } else {
            LOG.error("poolMemAlloc() freeQueue: " + freeQueue.size() + " inUseQueue: " + inUseQueue.size());
        }
        return null;
    }

    public void poolMemFree(final ByteBuffer buffer, final BufferManager bufferManager) {

        MemoryTracking tracker = removeTracker(buffer, bufferManager);
        if (tracker != null) {
            freeQueue.add(tracker);
        } else {
            if (bufferManager != null) {
                LOG.warn("Not on freeQueue " + bufferManager.getBufferManagerName());
            } else {
                LOG.warn("Not on freeQueue null BufferManager");
            }
        }
    }

    private MemoryTracking removeTracker(final ByteBuffer buffer, final BufferManager bufferManager) {
        for (MemoryTracking tracker : inUseQueue) {
            if (tracker.matches(buffer, bufferManager)) {
                inUseQueue.remove(tracker);
                return tracker;
            }
        }

        return null;
    }

    public int releaseBuffers(final String caller) {
        for (MemoryTracking tracker : freeQueue) {
            freeQueue.remove(tracker);

            tracker.clear();
        }

        /*
        ** Only dump out the information if there is an unexpected size for one of the queues
         */
        if ((freeQueue.size() != 0) || (inUseQueue.size() != 0)) {
            LOG.info("releaseBuffers() caller: " + caller + " freeQueue size: " + freeQueue.size() + " inUseQueue size: " +
                    inUseQueue.size());
        }

        return inUseQueue.size();
    }

    public void dumpInUseQueue() {

        for (MemoryTracking memoryTracking : inUseQueue) {
            LOG.warn("inUse " + memoryTracking.getOwner());
        }
    }
}

