package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.ServerLoadBalancer;
import com.oracle.athena.webserver.server.ServerWorkerThread;
import com.oracle.pic.casper.webserver.server.WebServerAuths;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/*
** This thread pool is used for pipelines that have operations that access off box resources and
**   potentially would block the primary worker thread that is handling a connection.
 */
public class BlockingPipelineThreadPool {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingPipelineThreadPool.class);

    private final static int MAX_BLOCKING_WORKER_THREADS = 100;

    protected final WebServerFlavor flavor;
    protected final int numWorkerThreads;
    protected final int workQueueSize;
    protected final int blockingThreadBaseId;

    protected final BlockingWorkerThread[] threadPool;
    protected final BlockingQueue<ConnectionState> blockingWorkQueue;


    BlockingPipelineThreadPool(final WebServerFlavor flavor, final int queueSize, final int numWorkerThreads, final int blockingThreadBaseId) {
        this.flavor = flavor;
        this.workQueueSize = queueSize;
        this.blockingThreadBaseId = blockingThreadBaseId;

        /*
         ** Limit the maximum number of blocking worker threads to 100. That is still probably too many
         **   threads to be running when most systems have far fewer than 100 cores available
         **   to run workloads on.
         */
        if (numWorkerThreads > MAX_BLOCKING_WORKER_THREADS) {
            LOG.info("BlockingPipelineThreadPool[" + blockingThreadBaseId + "] workerThreads capped at: " + MAX_BLOCKING_WORKER_THREADS);
            this.numWorkerThreads = MAX_BLOCKING_WORKER_THREADS;
        } else {
            this.numWorkerThreads = numWorkerThreads;
        }

        this.threadPool = new BlockingWorkerThread[this.numWorkerThreads];

        blockingWorkQueue = new LinkedBlockingQueue<>(this.workQueueSize);
    }

    void start() {
        for (int i = 0; i < numWorkerThreads; i++) {
            BlockingWorkerThread worker = new BlockingWorkerThread(blockingWorkQueue, (blockingThreadBaseId + i));
            worker.start();
            threadPool[i] = worker;
        }
    }

    void stop() {
        for (int i = 0; i < numWorkerThreads; i++) {
            BlockingWorkerThread worker = threadPool[i];
            threadPool[i] = null;
            if (worker == null) {
                LOG.info("BlockingPipelineThreadPool[" + blockingThreadBaseId + "] stop (worker == null) i: " + i);
                break;
            }
            worker.stop();
        }
    }

    /*
    ** There is a single work queue for all of the BlockingWorkerThread(s). Any thread can pull work off and
    **   perform work on the ConnectionState.
     */
    void addBlockingWorkToThread(ConnectionState connectionState) {

        if (!blockingWorkQueue.offer(connectionState)) {
            LOG.error("Unable to offer() [" + connectionState.getConnStateId() + "]");
        } else {
            LOG.info("addComputeWorkToThread() [" + connectionState.getConnStateId() + "]");
        }
    }

}
