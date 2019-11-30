package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;
import com.oracle.athena.webserver.connectionstate.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ServerDigestThreadPool {

    private static final Logger LOG = LoggerFactory.getLogger(ServerDigestThreadPool.class);

    // TODO: Fix the based upon the number of connections
    private final static int SERVER_DIGEST_MAX_QUEUE_SIZE = 2 * ConnectionState.MAX_OUTSTANDING_BUFFERS;

    private final int threadCount;
    private final int baseThreadId;
    private final ServerDigestThread[] digestThreads;

    private final BlockingQueue<BufferState> digestWorkQueue;

    ServerDigestThreadPool(final int threadCount, final int baseThreadId){
        this.threadCount = threadCount;
        this.baseThreadId = baseThreadId;
        this.digestThreads = new ServerDigestThread[this.threadCount];

        digestWorkQueue = new LinkedBlockingQueue<>(SERVER_DIGEST_MAX_QUEUE_SIZE);

        LOG.error("ServerDigestThreadPool[" + this.baseThreadId + "] threadCount: " + this.threadCount);

    }

    public void start () {
        for (int i = 0; i < threadCount; i++) {
            ServerDigestThread digestThread = new ServerDigestThread(digestWorkQueue, (baseThreadId + i));
            digestThread.start();
            digestThreads[i] = digestThread;
        }
    }

    public void stop () {
        for (int i = 0; i < threadCount; i++) {
            digestThreads[i].stop();
        }
    }

    /*
     */
    void addDigestWorkToThread(BufferState bufferState) {

        if (!digestWorkQueue.offer(bufferState)) {
            LOG.error("Unable to offer() BufferState owner: [" + bufferState.getOwnerId() + "]");
        } else {
            LOG.info("addDigestWorkToThread() BufferState owner: [" + bufferState.getOwnerId() + "]");
        }
    }
}
