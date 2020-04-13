package com.webutils.webserver.memory;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.operations.OperationTypeEnum;

import java.nio.ByteBuffer;

public class MemoryTracking {

    private final int identifier;
    private OperationTypeEnum allocator;
    private ByteBuffer buffer;
    private BufferManager bufferManager;

    MemoryTracking(final ByteBuffer buffer, final int identifier) {
        this.buffer = buffer;
        this.identifier = identifier;
        this.bufferManager = null;
    }

    void setBufferManager(final BufferManager bufferMgr, final OperationTypeEnum allocator) {
        this.bufferManager = bufferMgr;
        this.allocator = allocator;
    }

    ByteBuffer getBuffer() {
        return buffer;
    }

    /*
    ** This has the side effect of clearing out the bufferManager and allocator if it returns true
    ** It also resets the pointers in the ByteBuffer
     */
    boolean matches(final ByteBuffer bufferToRelease, final BufferManager releaseBufferMgr) {
        /*
        ** Need to handle the case where the ClientTest memory allocations use a BufferManager of null
         */
        if (bufferManager != null) {
            if (buffer.equals(bufferToRelease) && bufferManager.equals(releaseBufferMgr)) {
                buffer.clear();
                bufferManager = null;
                allocator = null;
                return true;
            } else {
                return false;
            }
        } else {
            if (buffer.equals(bufferToRelease)) {
                buffer.clear();
                allocator = null;
                return true;
            } else {
                return false;
            }

        }
    }

    String getOwner() {
        if (bufferManager != null) {
            return (bufferManager.getBufferManagerName() + " " + identifier + " " + allocator.toString());
        } else {
            return "null";
        }
    }

    public void clear() {
        buffer = null;
        bufferManager = null;
        allocator = null;
    }
}
