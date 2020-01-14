package com.oracle.athena.webserver.memory;

import java.nio.ByteBuffer;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic class to encapsulate memory management.
 * <p>
 * For now, it simply wraps calls to ByteBuffer.allocate().
 */
public class MemoryManager {
    // TODO:  Add a high-water mark for each pool.

    private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

    // These are publicly visible and used in the buffer allocation calls.
    public static final int MEDIUM_BUFFER_SIZE = 0x400;  // 1 kB;
    public static final int XFER_BUFFER_SIZE = 0x2000;  // 8 kB

    // These really don't need to be.
    private static final int PRODUCTION_XFER_BUFFER_COUNT = 10000;

    private static final int INTEGRATION_XFER_BUFFER_COUNT = 400;

    // Or we could infer this from an array of {count, size} tuples, if we didn't
    // need to expose the threshold values.
    private FixedSizeBufferPool memoryPool;

    public MemoryManager(WebServerFlavor flavor) {
        int xferBufferCount;

        /*
        ** The number of buffers allocated in each pool needs to be smaller for the
        **    WebServerFlavor.INTEGRATION configuration to allow it to run on the
        **    desktop.
         */
        if (flavor == WebServerFlavor.STANDARD) {
            xferBufferCount = PRODUCTION_XFER_BUFFER_COUNT;
        } else {
            xferBufferCount = INTEGRATION_XFER_BUFFER_COUNT;
        }

        memoryPool = new FixedSizeBufferPool(XFER_BUFFER_SIZE, xferBufferCount );
    }

    public ByteBuffer poolMemAlloc(final int requestedSize, final BufferManager bufferManager) {

        return memoryPool.poolMemAlloc(bufferManager);
    }

    public void poolMemFree(ByteBuffer buffer, final BufferManager bufferManager) {
        memoryPool.poolMemFree(buffer, bufferManager);
    }

    /*
    ** This is to check that the memory pools are completely populated. It is used to validate that tests and such
    **   are not leaking resources.
     */
    public boolean verifyMemoryPools(final String caller) {
        boolean memoryPoolsOkay = true;

        if (memoryPool.getBufferCount() != memoryPool.getUnusedBufferCount()) {
            LOG.warn(caller + " BufferPool error COUNT: " + memoryPool.getBufferCount() +
                    " unused: " + memoryPool.getUnusedBufferCount());

            memoryPool.dumpInUseQueue();
            memoryPoolsOkay = false;
        }

        return memoryPoolsOkay;
    }

}

