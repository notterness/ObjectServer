package com.oracle.athena.webserver.memory;

import sun.jvm.hotspot.opto.Block;

import java.nio.ByteBuffer;

/**
 * Basic class to encapsulate memory management.
 * <p>
 * For now, it simply wraps calls to ByteBuffer.allocate().
 */
public class MemoryManager {

    // TODO:  Make MemoryManager own an array of pools of various sizes.
    //        Add a high-water mark for each pool.
    public static final int SMALL_BUFFER_SIZE = 256;
    public static final int SMALL_BUFFER_COUNT = 256;
    public static final int MEDIUM_BUFFER_SIZE = 1024;
    public static final int MEDIUM_BUFFER_COUNT = 1024;

    private FixedSizeBufferPool thePool;

    public MemoryManager() {
        thePool = new FixedSizeBufferPool( MEDIUM_BUFFER_SIZE, MEDIUM_BUFFER_COUNT );
    }

    public ByteBuffer jniMemAlloc(int bufferSize) {
        return thePool.jniMemAlloc( bufferSize );
    }

    public void jniMemFree(ByteBuffer buffer) {
        thePool.jniMemFree( buffer );
    }
}

