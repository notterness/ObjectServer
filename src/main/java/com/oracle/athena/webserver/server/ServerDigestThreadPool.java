package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerDigestThreadPool {

    protected final ExecutorService digestThreadPool;
    protected int lastCurrentThread;
    private final int threadCount;
    private final ServerDigestThread[] digestThreads;

    public ServerDigestThreadPool(final int threadCount){
        this.threadCount = threadCount;
        digestThreads    = new ServerDigestThread[this.threadCount];
        digestThreadPool = Executors.newFixedThreadPool(this.threadCount);
        this.lastCurrentThread = 0;
    }

    public void start () {
        for (int i = 0; i < threadCount; i++) {
            ServerDigestThread thread = new ServerDigestThread();
            digestThreadPool.execute(thread);
            digestThreads[i] = thread;
        }
    }

    public void stop () {
        for (int i = 0; i < threadCount; i++) {
            digestThreads[i].stopServerDigestThread();
        }

        digestThreadPool.shutdown();
    }

    /*
     ** simple round robin add to the digest thread for now.
     */
    public boolean addDigestWorkToThread(BufferState bufState) {
        int currentThread = lastCurrentThread;
        boolean isQueuedToDigestQ = false;

        while (!isQueuedToDigestQ) {
            /*
            **  addDigestWork returns true if the work is queued. Try another worker, if there isn't
            **  space right now.
            */
            isQueuedToDigestQ = digestThreads[currentThread].addDigestWork(bufState);
            currentThread++;
            if (currentThread == threadCount) {
                currentThread = 0;
            }

            /*
            ** as to not block the worker queue, just return false and do the wait on the ServerWorkerThread
            */
            if (currentThread == lastCurrentThread) {
                break;
            }
        }
        lastCurrentThread = currentThread;
        return isQueuedToDigestQ;
    }
}
