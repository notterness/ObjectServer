package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;

import java.util.concurrent.BlockingQueue;

class ServerDigestThread extends ComputeThread {

    ServerDigestThread(final BlockingQueue<BufferState> workQueue, int threadId) {
        super(workQueue, threadId);
    }

    @Override
    void performComputeTask(BufferState bufferState) {
        /*
         ** compute the digest for the buffer.
         */
        bufferState.updateBufferDigest();
    }

    @Override
    void performCallback(BufferState bufferState){
        /*
         **  this call queues the buffer back to the server worker. Changes to the state are made on that thread.
         */
        bufferState.bufferCompleteDigestCb();
    }

}
