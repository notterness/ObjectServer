package com.oracle.athena.webserver.memory;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.MemoryAvailableCallback;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Basic class to encapsulate memory management.
 * <p>
 * For now, it simply wraps calls to ByteBuffer.allocate().
 */
public class MemoryManager {
    // TODO:  Make MemoryManager own an array of pools of various sizes.
    //        Add a high-water mark for each pool.

    private BlockingQueue<MemoryAvailableCallback> waitingForBuffersQueue;

    // These are publicly visible.
    public static final int SMALL_BUFFER_SIZE = 256;
    public static final int MEDIUM_BUFFER_SIZE = 1024;
    public static final int LARGE_BUFFER_SIZE = 0x100000;  // 1 MB
    public static final int CHUNK_BUFFER_SIZE = 0x8000000;  // 128 MB

    // These really don't need to be.
    public static final int SMALL_BUFFER_COUNT = 10000;
    public static final int MEDIUM_BUFFER_COUNT = 1000;
    public static final int LARGE_BUFFER_COUNT =  100;
    public static final int CHUNK_BUFFER_COUNT =    2;  // If this is more than 2, get OOM in unit test.

    /*
    ** TODO: This should be the number of ConnectionState objects
     */
    private static final int MAX_WAITING_MEMORY_CONNECTIONS = 1000;

    // Or we could infer this from an array of {count, size} tuples, if we didn't
    // need to expose the threshold values.
    private static final int numPools = 4;
    private FixedSizeBufferPool[] thePool;

    public MemoryManager() {
        thePool = new FixedSizeBufferPool[numPools];
        thePool[0] = new FixedSizeBufferPool( SMALL_BUFFER_SIZE,  SMALL_BUFFER_COUNT );
        thePool[1] = new FixedSizeBufferPool( MEDIUM_BUFFER_SIZE, MEDIUM_BUFFER_COUNT );
        thePool[2] = new FixedSizeBufferPool( LARGE_BUFFER_SIZE,  LARGE_BUFFER_COUNT );
        thePool[3] = new FixedSizeBufferPool( CHUNK_BUFFER_SIZE,  CHUNK_BUFFER_COUNT );

        waitingForBuffersQueue = new LinkedBlockingQueue<>(MAX_WAITING_MEMORY_CONNECTIONS);
    }

    public ByteBuffer poolMemAlloc(final int requestedSize, MemoryAvailableCallback memFreeCb) {

        /*
        ** TODO: If there are already ConnectionState waiting for memory, need to wait for those to
        **   be handled first
         */
        ByteBuffer buffer = null;

        for (int i = 0; i < numPools;  ++i) {
            if (requestedSize <= thePool[i].getBufferSize()) {
                buffer = thePool[i].poolMemAlloc( requestedSize );
                if ((buffer == null) && (memFreeCb != null)) {
                    try {
                        waitingForBuffersQueue.put(memFreeCb);
                    } catch (InterruptedException int_ex) {
                        int_ex.printStackTrace();
                    }
                }
            }
        }
        return buffer;
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

