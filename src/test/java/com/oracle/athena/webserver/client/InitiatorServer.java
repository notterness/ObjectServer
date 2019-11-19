package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.server.ServerChannelLayer;

public class InitiatorServer extends ServerChannelLayer {

    private InitiatorLoadBalancer serverWorkHandler;

    private int clientId;
    private int numberWorkerThreads;

    InitiatorServer(int workerThreads, int listenPort, int uniqueId) {
        super(workerThreads, listenPort, uniqueId);

        numberWorkerThreads = workerThreads;
        clientId = uniqueId;
    }

    public void start() {
        serverWorkHandler = new InitiatorLoadBalancer(ServerChannelLayer.WORK_QUEUE_SIZE, numberWorkerThreads, memoryManager,
                (clientId * 100));
        serverWorkHandler.start();

        serverAcceptThread = new Thread(this);
        serverAcceptThread.start();
    }

    InitiatorLoadBalancer getLoadBalancer() {
        System.out.println("ServerChannelLayer(" + clientId + ") getLoadBalancer() " + Thread.currentThread().getName());

        return serverWorkHandler;
    }

}
