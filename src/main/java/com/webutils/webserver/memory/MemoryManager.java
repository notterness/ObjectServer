package com.webutils.webserver.memory;

import java.nio.ByteBuffer;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.WebServerFlavor;
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

    public static final int TEST_BUFFER_MGR_RING_SIZE = 32;
    public static final int PRODUCTION_BUFFER_MGR_RING_SIZE = 4096;


    // These really don't need to be.
    private static final int PRODUCTION_XFER_BUFFER_COUNT = 10000;

    private static final int INTEGRATION_XFER_BUFFER_COUNT = 1000;

    private final WebServerFlavor webServerFlavor;

    // Or we could infer this from an array of {count, size} tuples, if we didn't
    // need to expose the threshold values.
    private FixedSizeBufferPool memoryPool;

    public MemoryManager(final WebServerFlavor flavor) {
        int xferBufferCount;

        this.webServerFlavor = flavor;

        /*
        ** The number of buffers allocated in each pool needs to be smaller for the
        **    WebServerFlavor.INTEGRATION configuration to allow it to run on the
        **    desktop.
         */
        if (webServerFlavor == WebServerFlavor.STANDARD) {
            xferBufferCount = PRODUCTION_XFER_BUFFER_COUNT;
        } else {
            xferBufferCount = INTEGRATION_XFER_BUFFER_COUNT;
        }

        memoryPool = new FixedSizeBufferPool(XFER_BUFFER_SIZE, xferBufferCount );
    }

    public ByteBuffer poolMemAlloc(final int requestedSize, final BufferManager bufferManager, final OperationTypeEnum allocator) {

        return memoryPool.poolMemAlloc(bufferManager, allocator);
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
        int unusedBufferCount = memoryPool.getUnusedBufferCount();

        /*
        ** Release all the memory in the unused Buffer Pool
         */
        int inUseCount = memoryPool.releaseBuffers(caller);

        if (inUseCount != 0) {
            LOG.warn(caller + " BufferPool error COUNT: " + memoryPool.getBufferCount() +
                    " unused: " + unusedBufferCount);

            memoryPool.dumpInUseQueue();
            memoryPoolsOkay = false;
        }

        return memoryPoolsOkay;
    }

    /*
     ** Value for the number of ByteBuffer(s) in the BufferManager rings
     */
    public int getBufferManagerRingSize() {
        if ((webServerFlavor == WebServerFlavor.DOCKER_OBJECT_SERVER_PRODUCTION) ||
                (webServerFlavor == WebServerFlavor.DOCKER_STORAGE_SERVER_PRODUCTION)) {
            return PRODUCTION_BUFFER_MGR_RING_SIZE;
        } else {
            return TEST_BUFFER_MGR_RING_SIZE;
        }
    }

}

