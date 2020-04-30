package com.webutils.webserver.buffermgr;

import com.webutils.webserver.operations.ChunkMetering;

public class ChunkAllocBufferInfo {

    private final BufferManager bufferManager;
    private final BufferManagerPointer addBufferPointer;

    private ChunkMetering metering;

    ChunkAllocBufferInfo(final BufferManager bufferMgr, final BufferManagerPointer addBufferPtr) {
        this.bufferManager = bufferMgr;
        this.addBufferPointer = addBufferPtr;
    }

    public void setMetering(final ChunkMetering chunkMetering) {
        metering = chunkMetering;
    }

    public ChunkMetering getMetering() { return metering; }

    public BufferManager getBufferManager() { return bufferManager; }

    public BufferManagerPointer getAddBufferPointer() { return addBufferPointer; }
}
