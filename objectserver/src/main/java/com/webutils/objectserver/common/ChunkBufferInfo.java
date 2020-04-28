package com.webutils.objectserver.common;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;

public class ChunkBufferInfo {

    private final int chunkNumber;
    private final int chunkSizeInBytes;

    private final BufferManager bufferManager;
    private final BufferManagerPointer inputPointer;

    public ChunkBufferInfo(final int chunkNumber, final int chunkSize, final BufferManager bufferManager,
                           final BufferManagerPointer inputPtr) {

        this.chunkNumber = chunkNumber;
        this.chunkSizeInBytes = chunkSize;
        this.bufferManager = bufferManager;
        this.inputPointer = inputPtr;
    }

    public int getChunkNumber() { return chunkNumber; }

    public int getChunkSizeInBytes() { return chunkSizeInBytes; }

    public BufferManager getChunkBufferManager() { return bufferManager; }

    public BufferManagerPointer getChunkDataPointer() { return inputPointer; }
}
