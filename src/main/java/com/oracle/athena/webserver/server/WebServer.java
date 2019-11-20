package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.memory.MemoryManager;

public class WebServer {

    private ServerChannelLayer http_server;
    private ServerChannelLayer https_server;
    private ServerLoadBalancer serverWorkHandler;
    private MemoryManager memoryManager;

    public WebServer(int workerThreads) {
        this(workerThreads, ServerChannelLayer.DEFAULT_CLIENT_ID);
    }

    public WebServer(int workerThreads, int serverClientId) {
        memoryManager = new MemoryManager();
        serverWorkHandler = new ServerLoadBalancer(2, workerThreads, memoryManager,
                (serverClientId * 100));

        http_server = new ServerChannelLayer(serverWorkHandler, ServerChannelLayer.HTTP_TCP_PORT, serverClientId);
        https_server = new ServerChannelLayer(serverWorkHandler, ServerChannelLayer.HTTPS_TCP_PORT, serverClientId + 1);
    }

    public WebServer(int workerThreads, int listenPort, int serverClientId) {
        memoryManager = new MemoryManager();

        serverWorkHandler = new ServerLoadBalancer(2, workerThreads, memoryManager,
                (serverClientId * 100));

        http_server = new ServerChannelLayer(serverWorkHandler, listenPort, serverClientId);
        https_server = new ServerChannelLayer(serverWorkHandler, listenPort + 80, serverClientId + 1);
    }

    public void start() {
        /*
        ** The queueSize is set to 2 to insure that the system runs out of connections and can be tested for
        **   the out of connections handling.
         */
        serverWorkHandler.start();
        http_server.start();
        https_server.start();
    }

    public void stop() {
        /*
         ** The queueSize is set to 2 to insure that the system runs out of connections and can be tested for
         **   the out of connections handling.
         */
        serverWorkHandler.stop();
        http_server.stop();
        https_server.stop();
    }

}

