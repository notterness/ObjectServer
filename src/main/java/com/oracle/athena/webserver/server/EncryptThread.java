package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;

import java.util.concurrent.BlockingQueue;

public class EncryptThread extends ComputeThread {

    EncryptThread(final BlockingQueue<BufferState> workQueue, int threadId) {
        super(workQueue, threadId);
    }

    @Override
    void performComputeTask(BufferState bufferState) {
        /*
         ** Encrypt the buffer while copying it to a Chunk for eventual writing out
         **   to the Storage Server.
         */
    }

    @Override
    void performCallback(BufferState bufferState){
        /*
         **  this call queues the buffer back to the server worker. Changes to the state are made on that thread.
         */
        bufferState.bufferEncryptCompleteCb();
    }

}
