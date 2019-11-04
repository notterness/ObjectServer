package com.oracle.athena.webserver.memory;

import java.nio.ByteBuffer;

/**
 * Basic class to encapsulate memory management.
 * <p>
 * For now, it simply wraps calls to ByteBuffer.allocate().
 */
public class MemoryManager {

    public static final int SMALL_BUFFER_SIZE = 256;
    public static final int MEDIUM_BUFFER_SIZE = 1024;

    public ByteBuffer jniMemAlloc(int bufferSize) {
        // TODO instead of calling allocate, perhaps grab a ByteBuffer chunk out of a larger cache
        return ByteBuffer.allocate(bufferSize);
    }

    public void jniMemFree(ByteBuffer buffer) {
        buffer.clear();
    }
}

