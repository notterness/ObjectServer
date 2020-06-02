package com.webutils.webserver.buffermgr;

/*
** This is used to provide an association between a BufferManager and a BufferManagerPointer
**   to allow them to be stored away easily.
 */
public class BufferAssociation {

    private final BufferManager bufferManager;
    private final BufferManagerPointer bufferManagerPointer;

    public BufferAssociation(final BufferManager bufferManager, final BufferManagerPointer bufferManagerPointer) {
        this.bufferManager = bufferManager;
        this.bufferManagerPointer = bufferManagerPointer;
    }

    public BufferManager getBufferManager() {
        return bufferManager;
    }

    public BufferManagerPointer getBufferManagerPointer() {
        return bufferManagerPointer;
    }
}
