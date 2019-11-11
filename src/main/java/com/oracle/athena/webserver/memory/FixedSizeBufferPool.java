package com.oracle.athena.webserver.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Basic class to encapsulate memory management.
 * <p>
 */
public class FixedSizeBufferPool {

    final private int bufferSize;
    final private int bufferCount;

    final private BlockingQueue<ByteBuffer> freeQueue;
    final private BlockingQueue<ByteBuffer> inuseQueue;

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

    public int getNumBuffersInUse() { return inuseQueue.size(); }

    public ByteBuffer poolMemAlloc(int bufferSize) {
        // Instead of calling allocate, perhaps grab a ByteBuffer chunk out of a larger cache
        // If none available, will return null and queue up callback (if supplied) for next free.
        ByteBuffer ret = freeQueue.poll();
        if (ret != null) {
            inuseQueue.add(ret);
        }
        // } catch (InterruptedException int_ex ) {
        //    System.out.println( int_ex.getMessage());
        // }
        // System.out.println( "Size of inuseQueue is " + inuseQueue.size() );
        return ret;
    }

    public void poolMemFree(ByteBuffer buffer) {
        if (inuseQueue.remove( buffer )) {
            buffer.clear();
            freeQueue.add( buffer );
        }
        // System.out.println( "Size of inuseQueue is " + inuseQueue.size() );
    }
}

