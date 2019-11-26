package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.ServerChannelLayer;
import com.oracle.athena.webserver.server.ServerDigestThreadPool;

public class InitiatorServer extends ServerChannelLayer {

    private InitiatorLoadBalancer serverWorkHandler;

    private int clientId;
    private int numberWorkerThreads;
    private MemoryManager memoryManager;
    private ServerDigestThreadPool digestThreadPool;

    InitiatorServer(int workerThreads, int listenPort, int uniqueId) {
        super(listenPort, uniqueId);

        this.numberWorkerThreads = workerThreads;
        this.clientId = uniqueId;

        this.memoryManager = new MemoryManager();
        this.digestThreadPool = new ServerDigestThreadPool(2, 10);
    }

    public void start() {
        serverWorkHandler = new InitiatorLoadBalancer(ServerChannelLayer.WORK_QUEUE_SIZE, this.numberWorkerThreads, this.memoryManager,
                (clientId * 100), this.digestThreadPool);
        serverWorkHandler.start();

        serverAcceptThread = new Thread(this);
        serverAcceptThread.start();
    }

    public void stop() {
        if (this.memoryManager.verifyMemoryPools("Initiator Server")) {
            System.out.println("Initiator Server[" + (clientId * 100) + "] Memory Verification All Passed");
        }
    }

    InitiatorLoadBalancer getLoadBalancer() {
        System.out.println("ServerChannelLayer(" + clientId + ") getLoadBalancer() " + Thread.currentThread().getName());

        return serverWorkHandler;
    }

}
