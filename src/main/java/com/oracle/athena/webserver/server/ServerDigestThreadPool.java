package com.oracle.athena.webserver.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDigestThreadPool extends ComputeThreadPool {
    private static final Logger LOG = LoggerFactory.getLogger(ServerDigestThreadPool.class);


    ServerDigestThreadPool(final int threadCount, final int baseThreadId){
        super(threadCount, baseThreadId);
        LOG.error("ServerDigestThreadPool[" + baseThreadId + "] threadCount: " + threadCount);
    }

    @Override
    public void start () {
        for (int i = 0; i < threadCount; i++) {
            ServerDigestThread digestThread = new ServerDigestThread(computeWorkQueue, (baseThreadId + i));
            digestThread.start();
            computeThreads[i] = digestThread;
        }
    }

}
