package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;
import com.oracle.athena.webserver.connectionstate.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

abstract class ComputeThreadPool {

    private static final Logger LOG = LoggerFactory.getLogger(ComputeThreadPool.class);

    // TODO: Fix the based upon the number of connections
    private final static int COMPUTE_MAX_QUEUE_SIZE = 2 * ConnectionState.MAX_OUTSTANDING_BUFFERS;

    protected final int threadCount;
    protected final int baseThreadId;
    protected final ComputeThread[] computeThreads;
    protected final BlockingQueue<BufferState> computeWorkQueue;

    ComputeThreadPool(final int threadCount, final int baseThreadId){
        this.threadCount = threadCount;
        this.baseThreadId = baseThreadId;
        this.computeThreads = new ComputeThread[this.threadCount];

        computeWorkQueue = new LinkedBlockingQueue<>(COMPUTE_MAX_QUEUE_SIZE);

        LOG.error("ComputeThreadPool[" + this.baseThreadId + "] threadCount: " + this.threadCount);

    }

    abstract void start ();

    void stop () {
        for (int i = 0; i < threadCount; i++) {
            computeThreads[i].stop();
        }
    }

    /*
     */
    void addComputeWorkToThread(BufferState bufferState) {

        if (!computeWorkQueue.offer(bufferState)) {
            LOG.error("Unable to offer() BufferState owner: [" + bufferState.getOwnerId() + "]");
        } else {
            LOG.info("addComputeWorkToThread() BufferState owner: [" + bufferState.getOwnerId() + "]");
        }
    }
}
