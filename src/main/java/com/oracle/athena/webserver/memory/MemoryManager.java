package com.oracle.athena.webserver.memory;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.MemoryAvailableCallback;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic class to encapsulate memory management.
 * <p>
 * For now, it simply wraps calls to ByteBuffer.allocate().
 */
public class MemoryManager {
    // TODO:  Add a high-water mark for each pool.

    private static Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

    private BlockingQueue<MemoryAvailableCallback> waitingForBuffersQueue;

    // These are publicly visible.
    // TODO: Get rid of names from the interface.
    public static final int SMALL_BUFFER_SIZE = 0x100;  // 256
    public static final int MEDIUM_BUFFER_SIZE = 0x400;  // 1 kB;
    public static final int XFER_BUFFER_SIZE = 0x10000;  // 64 kB
    public static final int LARGE_BUFFER_SIZE = 0x100000;  // 1 MB
    public static final int CHUNK_BUFFER_SIZE = 0x8000000;  // 128 MB

    // These really don't need to be.
    public static final int SMALL_BUFFER_COUNT = 1000;
    public static final int MEDIUM_BUFFER_COUNT = 100;
    public static final int XFER_BUFFER_COUNT = 10;
    public static final int LARGE_BUFFER_COUNT =  10;
    public static final int CHUNK_BUFFER_COUNT =    1;  // If this is more than 2, get OOM in unit test.

    /*
    ** TODO: This should be the number of ConnectionState objects
     */
    private static final int MAX_WAITING_MEMORY_CONNECTIONS = 1000;

    // Or we could infer this from an array of {count, size} tuples, if we didn't
    // need to expose the threshold values.
    private static final int numPools = 5;
    private FixedSizeBufferPool[] thePool;

    public MemoryManager() {
        thePool = new FixedSizeBufferPool[numPools];
        thePool[0] = new FixedSizeBufferPool( SMALL_BUFFER_SIZE,  SMALL_BUFFER_COUNT );
        thePool[1] = new FixedSizeBufferPool( MEDIUM_BUFFER_SIZE, MEDIUM_BUFFER_COUNT );
        thePool[2] = new FixedSizeBufferPool( XFER_BUFFER_SIZE,   XFER_BUFFER_COUNT );
        thePool[3] = new FixedSizeBufferPool( LARGE_BUFFER_SIZE,  LARGE_BUFFER_COUNT );
        thePool[4] = new FixedSizeBufferPool( CHUNK_BUFFER_SIZE,  CHUNK_BUFFER_COUNT );

        waitingForBuffersQueue = new LinkedBlockingQueue<>(MAX_WAITING_MEMORY_CONNECTIONS);
    }

    public ByteBuffer poolMemAlloc(final int requestedSize, MemoryAvailableCallback memFreeCb) {

        /*
        ** TODO: If there are already ConnectionState waiting for memory, need to wait for those to
        **   be handled first
         */
        for (int i = 0; i < numPools;  ++i) {
            if (requestedSize <= thePool[i].getBufferSize()) {

                LOG.debug("poolMemAlloc [" + i + "] requestedSize: " + requestedSize);
                ByteBuffer buffer = thePool[i].poolMemAlloc( requestedSize );
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
}

class PoolArrayEntry {
    final int buffSize;
    final int buffCount;
    final FixedSizeBufferPool buffPool;

    PoolArrayEntry( int bufferSize, int bufferCount, FixedSizeBufferPool bufferPool ) {
        buffSize = bufferSize;
        buffCount = bufferCount;
        buffPool = bufferPool;
    }
}

