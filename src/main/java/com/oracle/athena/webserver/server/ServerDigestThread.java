package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ServerDigestThread implements Runnable {
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
                    calculateDigest(bufferWork);
                    bufferWork.setBufferStateDigestDone();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void stopServerDigestThread() {
        running = false;

    }

    public boolean addDigestWork(BufferState bufferState) {
        boolean isQueued;
        isQueued = digestWorkQ.offer(bufferState);

        return isQueued;
    }

    public void calculateDigest(BufferState bufferState) {
    }

}
