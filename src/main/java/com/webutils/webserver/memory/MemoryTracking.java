package com.webutils.webserver.memory;

import com.webutils.webserver.buffermgr.BufferManager;

import java.nio.ByteBuffer;

public class MemoryTracking {

    private final int identifier;
    private ByteBuffer buffer;
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

    public void clear() {
        buffer = null;
        bufferManager = null;
    }
}
