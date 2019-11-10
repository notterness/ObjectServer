package com.oracle.athena.webserver.memory;

import java.nio.ByteBuffer;

/**
 * Basic class to encapsulate memory management.
 * <p>
 * For now, it simply wraps calls to ByteBuffer.allocate().
 */
public class MemoryManager {
    // TODO:  Make MemoryManager own an array of pools of various sizes.
    //        Add a high-water mark for each pool.

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
    }

    public ByteBuffer poolMemAlloc(int requestedSize) {
        for (int i = 0; i < numPools;  ++i) {
            if (requestedSize <= thePool[i].getBufferSize()) {
                return thePool[i].poolMemAlloc( requestedSize );
            }
        }
        return null;
    }

    public void poolMemFree(ByteBuffer buffer) {
        for (int i = 0; i < numPools;  ++i) {
            if (buffer.capacity() <= thePool[i].getBufferSize()) {
                thePool[i].poolMemFree( buffer );
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

