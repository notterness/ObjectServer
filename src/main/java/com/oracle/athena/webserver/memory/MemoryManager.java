package com.oracle.athena.webserver.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    private BlockingQueue<MemoryAvailableCallback> waitingForBuffersQueue;

    // These are publicly visible and used in the buffer allocation calls.
    public static final int MEDIUM_BUFFER_SIZE = 0x400;  // 1 kB;
    public static final int XFER_BUFFER_SIZE = 0x2000;  // 8 kB

    // These really don't need to be.
    private static final int PRODUCTION_MEDIUM_BUFFER_COUNT = 100;
    private static final int PRODUCTION_XFER_BUFFER_COUNT = 10;

    private static final int INTEGRATION_MEDIUM_BUFFER_COUNT = 10;
    private static final int INTEGRATION_XFER_BUFFER_COUNT = 100;

    /* In sorted order */
    private static final int[] poolBufferSizes = {
        MEDIUM_BUFFER_SIZE,
        XFER_BUFFER_SIZE,
    };

    /*
    ** TODO: This should be the number of ConnectionState objects
     */
    private static final int MAX_WAITING_MEMORY_CONNECTIONS = 100;

    // Or we could infer this from an array of {count, size} tuples, if we didn't
    // need to expose the threshold values.
    private static final int numPools = 2;
    private FixedSizeBufferPool[] thePool;

    public MemoryManager(WebServerFlavor flavor) {
        int smallBufferCount;
        int mediumBufferCount;
        int xferBufferCount;

        /*
        ** The number of buffers allocated in each pool needs to be smaller for the
        **    WebServerFlavor.INTEGRATION configuration to allow it to run on the
        **    desktop.
         */
        if (flavor == WebServerFlavor.STANDARD) {
            mediumBufferCount = PRODUCTION_MEDIUM_BUFFER_COUNT;
            xferBufferCount = PRODUCTION_XFER_BUFFER_COUNT;
        } else {
            mediumBufferCount = INTEGRATION_MEDIUM_BUFFER_COUNT;
            xferBufferCount = INTEGRATION_XFER_BUFFER_COUNT;
        }

        thePool = new FixedSizeBufferPool[numPools];
        thePool[0] = new FixedSizeBufferPool( MEDIUM_BUFFER_SIZE, mediumBufferCount );
        thePool[1] = new FixedSizeBufferPool( XFER_BUFFER_SIZE, xferBufferCount );

        waitingForBuffersQueue = new LinkedBlockingQueue<>(MAX_WAITING_MEMORY_CONNECTIONS);
    }

    public ByteBuffer poolMemAlloc(final int requestedSize, MemoryAvailableCallback memFreeCb,
                                   final BufferManager bufferManager) {

        /*
        ** TODO: If there are already ConnectionState waiting for memory, need to wait for those to
        **   be handled first
         */
        for (int i = 0; i < numPools;  ++i) {
            if (requestedSize <= thePool[i].getBufferSize()) {

                LOG.debug("poolMemAlloc [" + i + "] requestedSize: " + requestedSize);
                ByteBuffer buffer = thePool[i].poolMemAlloc(requestedSize, bufferManager);
                if (buffer != null) {
                    return buffer;
                } else if (memFreeCb != null) {
                    try {
                        waitingForBuffersQueue.put(memFreeCb);
                    } catch (InterruptedException int_ex) {
                        int_ex.printStackTrace();
                    }
                }
                return null;
            }
        }

        // Didn't find a matching fixed-size pool for the requested buffer size.
        throw new IllegalArgumentException("Requested memory buffer size exceeds max: " + requestedSize );
    }

    public void poolMemFree(ByteBuffer buffer) {
        for (int i = 0; i < numPools;  ++i) {
            if (buffer.capacity() <= thePool[i].getBufferSize()) {
                thePool[i].poolMemFree( buffer );

                /*
                ** Are there waiters for memory?
                 */
                MemoryAvailableCallback memFreeCb = waitingForBuffersQueue.peek();
                if (memFreeCb != null) {
                    /*
                    ** TODO: Are there enough free buffers to handle the request?
                     */
                    int requestedBuffers = memFreeCb.connRequestedBuffers();

                    waitingForBuffersQueue.remove(memFreeCb);

                    memFreeCb.memoryAvailable();
                }
            }
        }
    }

    public void cancelCallback( MemoryAvailableCallback memFreeCb ) {
        waitingForBuffersQueue.remove(memFreeCb);
    }

    /*
    ** This is to check that the memory pools are completely populated. It is used to validate that tests and such
    **   are not leaking resources.
     */
    public boolean verifyMemoryPools(final String caller) {
        boolean memoryPoolsOkay = true;

        for (int i = 0; i < numPools; ++i) {
            if (thePool[i].getBufferCount() != thePool[i].getUnusedBufferCount()) {
                LOG.warn(caller + " BufferPool error pool[" + i + "] COUNT: " + thePool[i].getBufferCount() +
                        " unused: " + thePool[i].getUnusedBufferCount());

                thePool[i].dumpMap();
                memoryPoolsOkay = false;
            }
        }

        return memoryPoolsOkay;
    }

    static public int allocatedBufferCapacity(int requestedSize) {
        for(int i=0; i < poolBufferSizes.length; i++) {
            if (requestedSize <= poolBufferSizes[i])
                return poolBufferSizes[i];
        }

        return poolBufferSizes[poolBufferSizes.length - 1];
    }
}

