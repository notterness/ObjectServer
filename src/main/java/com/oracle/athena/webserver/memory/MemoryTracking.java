package com.oracle.athena.webserver.memory;

import com.oracle.athena.webserver.buffermgr.BufferManager;

import java.nio.ByteBuffer;

public class MemoryTracking {

    private final ByteBuffer buffer;
    private final int identifier;
    private BufferManager bufferManager;

    MemoryTracking(final ByteBuffer buffer, final int identifier) {
        this.buffer = buffer;
        this.identifier = identifier;
        this.bufferManager = null;
    }

    void setBufferManager(final BufferManager bufferMgr) {
        this.bufferManager = bufferMgr;
    }

    ByteBuffer getBuffer() {
        return buffer;
    }

    String getOwner() {
        if (bufferManager != null) {
            return bufferManager.getBufferManagerName();
        } else {
            return "null";
        }
    }

}
