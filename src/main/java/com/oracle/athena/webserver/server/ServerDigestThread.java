package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDigestThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerDigestThread.class);

    private final BlockingQueue<BufferState> digestWorkQ;
    private final int threadId;

    private volatile boolean running;
    private Thread digestThread;


    public ServerDigestThread(final BlockingQueue<BufferState> workQueue, int threadId) {
        this.digestWorkQ = workQueue;
        this.threadId = threadId;

        running = true;
    }

    public void start() {
        digestThread = new Thread(this);
        digestThread.start();
    }

    void stop() {
        running = false;
        try {
            digestThread.join(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("digestThread[] failed: " + int_ex.getMessage());
        }
    }

    public void run() {
        LOG.info("ServerDigestThread[" + threadId + "] started");

        while (running) {
            BufferState bufferWork;

            try {
                bufferWork = digestWorkQ.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException int_ex) {
                LOG.warn("ServerDigestThread[" + threadId + "] " + int_ex.getMessage());
                bufferWork = null;
                running = false;
            }

            if (bufferWork != null) {
                LOG.info("ServerDigestThread[" + threadId + "] buffer from: " + bufferWork.getOwnerId());
                /*
                ** compute the digest for the buffer.
                 */
                bufferWork.updateBufferDigest();

                /*
                **  this call queues the buffer back to the server worker. Changes to the state are made on that thread.
                 */
                bufferWork.bufferCompleteDigestCb();
            }
        }

        LOG.info("ServerDigestThread[" + threadId + "] finished");
    }

}
