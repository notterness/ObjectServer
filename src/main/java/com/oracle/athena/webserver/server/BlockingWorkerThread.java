package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.requestcontext.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/*
** These are threads used to run Pipelines that are accessing off box resources that would block the
**   ServerWorkerThreads.
 */
class BlockingWorkerThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingWorkerThread.class);

    private final int blockingThreadId;

    private volatile boolean running;
    private Thread blockingThread;

    private final BlockingQueue<RequestContext> blockingWorkQ;


    BlockingWorkerThread(final BlockingQueue<RequestContext> workQueue, final int blockingThreadId) {

        this.blockingWorkQ = workQueue;
        this.blockingThreadId = blockingThreadId;
    }

    void start() {
        running = true;
        blockingThread = new Thread(this);
        blockingThread.start();
    }

    void stop() {
        running = false;

        try {
            blockingThread.join(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("BlockingWorkerThread[" + blockingThreadId + "] failed: " + int_ex.getMessage());
        }
    }

    /*
     ** This is the method that does the actual work for this thread. It handles multiple
     **   connections at once. The expectation is that the pipelines that are using this thread
     **   are running operations that are blocking.
     */
    public void run() {

        LOG.info("BlockingWorkerThread[" + blockingThreadId + "] start " + Thread.currentThread().getName());

        while (running) {
            RequestContext work;

            try {
                work = blockingWorkQ.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException int_ex) {
                LOG.warn("BlockingWorkerThread[" + blockingThreadId + "] " + int_ex.getMessage());
                work = null;
                running = false;
            }

            if (work != null) {
            }
        }

        LOG.info("BlockingWorkerThread[" + blockingThreadId + "] exit " + Thread.currentThread().getName());
    }

}
