package com.oracle.athena.webserver.threadpools;

import com.oracle.athena.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/*
** ComputeThread provides a base class for operations that are CPU intensive and
**   should not be run on the primary worker threads. Since they are CPU intensive,
**   the total number of ComputeThreads should be kept to a relatively small number
**   (i.e. something around 4). In addition, these threads perform work for multiple
**   connections. Currently, the ComputeThread(s) are used for:
**      - Md5 Digest
**      - Encryption
 */
class ComputeThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ComputeThread.class);

    private final ComputeThreadPool poolOwner;
    private final int threadId;

    private volatile boolean running;
    private Thread computeThread;

    public ComputeThread(final ComputeThreadPool poolOwner, final int threadId) {
        this.poolOwner = poolOwner;
        this.threadId = threadId;

        running = true;
    }

    public void start() {
        computeThread = new Thread(this);
        computeThread.start();
    }

    void stop() {
        running = false;
        try {
            computeThread.join(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("ComputeThread[] failed: " + int_ex.getMessage());
        }
    }

    public void run() {
        LOG.info("ComputeThread[" + threadId + "] started");

        while (running) {
            running = poolOwner.executeComputeWork();
        }

        LOG.info("ComputeThread[" + threadId + "] finished");
    }

}
