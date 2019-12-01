package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;
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
abstract class ComputeThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerDigestThread.class);

    private final BlockingQueue<BufferState> digestWorkQ;
    private final int threadId;

    private volatile boolean running;
    private Thread computeThread;

    public ComputeThread(final BlockingQueue<BufferState> workQueue, int threadId) {
        this.digestWorkQ = workQueue;
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
            BufferState bufferWork;

            try {
                bufferWork = digestWorkQ.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException int_ex) {
                LOG.warn("ComputeThread[" + threadId + "] " + int_ex.getMessage());
                bufferWork = null;
                running = false;
            }

            if (bufferWork != null) {
                LOG.info("ComputeThread[" + threadId + "] buffer from: " + bufferWork.getOwnerId());

                /*
                 ** compute the digest for the buffer.
                 */
                performComputeTask(bufferWork);

                /*
                 **  this call queues the buffer back to the server worker. Changes to the state are made on that thread.
                 */
                performCallback(bufferWork);
            }
        }

        LOG.info("ComputeThread[" + threadId + "] finished");
    }

    abstract void performComputeTask(BufferState bufferState);
    abstract void performCallback(BufferState bufferState);

}
