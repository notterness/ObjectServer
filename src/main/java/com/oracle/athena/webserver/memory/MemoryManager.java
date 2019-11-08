package com.oracle.athena.webserver.memory;

import sun.jvm.hotspot.opto.Block;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Basic class to encapsulate memory management.
 * <p>
 * For now, it simply wraps calls to ByteBuffer.allocate().
 */
public class MemoryManager {

    public static final int SMALL_BUFFER_SIZE = 256;
    public static final int SMALL_BUFFER_COUNT = 256;
    public static final int MEDIUM_BUFFER_SIZE = 1024;
    public static final int MEDIUM_BUFFER__COUNT = 1024;

    private BlockingQueue<ByteBuffer> freeQueue;
    private BlockingQueue<ByteBuffer> inuseQueue;

    public MemoryManager() {
        freeQueue = new LinkedBlockingQueue<>(MEDIUM_BUFFER__COUNT) ;
        inuseQueue = new LinkedBlockingQueue<>(MEDIUM_BUFFER__COUNT) ;
        for (int i = 0; i < MEDIUM_BUFFER__COUNT; ++i) {
            freeQueue.add( ByteBuffer.allocateDirect( MEDIUM_BUFFER_SIZE ));
        }
    }

    public ByteBuffer jniMemAlloc(int bufferSize) {
        // TODO instead of calling allocate, perhaps grab a ByteBuffer chunk out of a larger cache
        ByteBuffer ret = null;
        try {
            ret = freeQueue.take();
            inuseQueue.add(ret);
        } catch (InterruptedException int_ex ) {
            System.out.println( int_ex.getMessage());
        }
        System.out.println( "Size of inuseQueue is " + inuseQueue.size() );
        return ret;
        // return ByteBuffer.allocateDirect(bufferSize);
    }

    public void jniMemFree(ByteBuffer buffer) {
        inuseQueue.remove( buffer );
        freeQueue.add( buffer );
        System.out.println( "Size of inuseQueue is " + inuseQueue.size() );
        // buffer.clear();
    }
}

