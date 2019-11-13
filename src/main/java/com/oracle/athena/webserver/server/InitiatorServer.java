package com.oracle.athena.webserver.server;

public class InitiatorServer extends ServerChannelLayer {

    private InitiatorLoadBalancer serverWorkHandler;

    public InitiatorServer(int workerThreads, int listenPort, int serverClientId) {
        super(workerThreads, listenPort, serverClientId);
    }

    public void start() {
        serverWorkHandler = new InitiatorLoadBalancer(ServerChannelLayer.WORK_QUEUE_SIZE, workerThreads, memoryManager,
                (serverClientId * 100));
        serverWorkHandler.start();

        serverAcceptThread = new Thread(this);
        serverAcceptThread.start();
    }

    public InitiatorLoadBalancer getLoadBalancer() {
        System.out.println("ServerChannelLayer(" + serverClientId + ") getLoadBalancer() " + Thread.currentThread().getName());

        return serverWorkHandler;
    }

}
