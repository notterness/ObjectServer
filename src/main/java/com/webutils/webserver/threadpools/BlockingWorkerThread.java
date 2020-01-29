package com.webutils.webserver.threadpools;

import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.requestcontext.RequestContext;
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

    private final BlockingQueue<Operation> blockingWorkQ;


    BlockingWorkerThread(final BlockingQueue<Operation> workQueue, final int blockingThreadId) {

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
     ** This is the method that does the actual work for this thread.
     **   The expectation is that the Operations that are using this thread are running operations that are blocking.
     **   By blocking, it means they are accessing resources that are off box and may take significant time
     **   to respond.
     */
    public void run() {

        LOG.info("BlockingWorkerThread[" + blockingThreadId + "] start " + Thread.currentThread().getName());

        while (running) {
            Operation blockingOperation;

            try {
                blockingOperation = blockingWorkQ.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException int_ex) {
                LOG.warn("BlockingWorkerThread[" + blockingThreadId + "] " + int_ex.getMessage());
                blockingOperation = null;
                running = false;
            }

            if (blockingOperation != null) {
                blockingOperation.execute();
            }
        }

        LOG.info("BlockingWorkerThread[" + blockingThreadId + "] exit " + Thread.currentThread().getName());
    }

}
