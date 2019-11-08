package com.oracle.athena.webserver.memory;

import sun.jvm.hotspot.opto.Block;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Basic class to encapsulate memory management.
 * <p>
 */
public class FixedSizeBufferPool {

    private int bufferSize;
    private int bufferCount;

    private BlockingQueue<ByteBuffer> freeQueue;
    private BlockingQueue<ByteBuffer> inuseQueue;

    public FixedSizeBufferPool( int bufferSize, int bufferCount ) {
        this.bufferSize = bufferSize;
        this.bufferCount = bufferCount;
        freeQueue = new LinkedBlockingQueue<>(bufferCount) ;
        inuseQueue = new LinkedBlockingQueue<>(bufferCount) ;
        for (int i = 0; i < bufferCount; ++i) {
            freeQueue.add( ByteBuffer.allocateDirect( bufferSize));
        }
    }

    public int getBufferSize() { return bufferSize; }

    public int getBufferCount() { return bufferCount; }

    public int getBuffersInUse() { return inuseQueue.size(); }

    public ByteBuffer jniMemAlloc(int bufferSize) {
        // TODO instead of calling allocate, perhaps grab a ByteBuffer chunk out of a larger cache
        ByteBuffer ret = null;
        try {
            // take() can block.  Don't really want that, but it shouldn't happen
            // if we check for empty before taking.
            if (!freeQueue.isEmpty()) {
                ret = freeQueue.take();
                inuseQueue.add(ret);
            } else {
                // OK, take this one off the heap as overflow.
                ret = ByteBuffer.allocate( bufferSize );
            }
        } catch (InterruptedException int_ex ) {
            System.out.println( int_ex.getMessage());
        }
        // System.out.println( "Size of inuseQueue is " + inuseQueue.size() );
        return ret;
        // return ByteBuffer.allocateDirect(bufferSize);
    }

    public void jniMemFree(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            // Must be one of ours.
            inuseQueue.remove( buffer );
            freeQueue.add( buffer );
            // System.out.println( "Size of inuseQueue is " + inuseQueue.size() );
            // buffer.clear();
        }
    }
}

