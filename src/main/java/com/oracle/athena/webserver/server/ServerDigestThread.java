package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDigestThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerDigestThread.class);

    private final static int SERVER_DIGEST_COUNT = 2;

    private BlockingQueue<BufferState> digestWorkQ;

    private boolean running;

    private int threadId;

    public ServerDigestThread() {
        digestWorkQ = new LinkedBlockingQueue<>(50);

    }

    @Override
    public void run() {
        running = true;

        try {
            while (running) {
                BufferState bufferWork = digestWorkQ.poll(100, TimeUnit.MILLISECONDS);

                if (bufferWork != null) {
                    /*
                     ** compute the digest for the buffer.
                     */
                    bufferWork.bufferUpdateDigest();
                    /*
                     ** FIXME PS - account for queue full.
                     */
                    digestWorkQ.remove(bufferWork);

                    /*
                     **  this call queues the buffer back to the server worker. Changes to the state are made on that thread.
                     */
                    bufferWork.bufferCompleteDigestCb();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void stopServerDigestThread() {
        running = false;
    }

    /*
     ** if there is no room here, we need to push this back to the server worker thread.
     */
    public boolean addDigestWork(BufferState bufferState) {
        boolean isQueued;
        isQueued = digestWorkQ.offer(bufferState);

        return isQueued;
    }
}
