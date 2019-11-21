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
        /*
        ** The queueSize is set to 2 to insure that the system runs out of connections and can be tested for
        **   the out of connections handling.
         */
        serverWorkHandler = new ServerLoadBalancer(2, workerThreads, memoryManager,
                (serverClientId * 100));
        serverWorkHandler.start();

        serverAcceptThread = new Thread(this);
        serverAcceptThread.start();
    }
}

