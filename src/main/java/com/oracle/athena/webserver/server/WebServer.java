package com.oracle.athena.webserver.server;

public class WebServer extends ServerChannelLayer {

    public WebServer(int workerThreads) {
        this(workerThreads, DEFAULT_CLIENT_ID);
    }

    public WebServer(int workerThreads, int serverClientId) {
        super(workerThreads, ServerChannelLayer.BASE_TCP_PORT, serverClientId);
    }

    public WebServer(int workerThreads, int listenPort, int serverClientId) {
        super(workerThreads, listenPort, serverClientId);
    }

    public void start() {
        serverWorkHandler = new ServerLoadBalancer(ServerChannelLayer.WORK_QUEUE_SIZE, workerThreads, memoryManager,
                (serverClientId * 100));
        serverWorkHandler.start();

        serverAcceptThread = new Thread(this);
        serverAcceptThread.start();
    }
}

