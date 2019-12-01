package com.oracle.athena.webserver.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptThreadPool extends ComputeThreadPool {
    private static final Logger LOG = LoggerFactory.getLogger(EncryptThreadPool.class);

    EncryptThreadPool(final int threadCount, final int baseThreadId){
        super(threadCount, baseThreadId);
        LOG.error("EncryptThreadPool[" + baseThreadId + "] threadCount: " + threadCount);
    }

    @Override
    public void start () {
        for (int i = 0; i < threadCount; i++) {
            EncryptThread encryptThread = new EncryptThread(computeWorkQueue, (baseThreadId + i));
            encryptThread.start();
            computeThreads[i] = encryptThread;
        }
    }

}

