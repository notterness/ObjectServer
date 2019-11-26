package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.memory.MemoryManager;

public class WebServer {

    private ServerChannelLayer http_server;
    private ServerChannelLayer https_server;
    private ServerLoadBalancer serverWorkHandler;
    private MemoryManager memoryManager;
    private ServerDigestThreadPool digestThreadPool;
    private int serverClientId;

    public WebServer(int workerThreads) {
        this(workerThreads, ServerChannelLayer.DEFAULT_CLIENT_ID);
    }

    public WebServer(int workerThreads, int serverClientId) {
        this.serverClientId = serverClientId;
        memoryManager = new MemoryManager();
        digestThreadPool = new ServerDigestThreadPool(2,10);
        serverWorkHandler = new ServerLoadBalancer(2, workerThreads, memoryManager, (serverClientId * 100),
                digestThreadPool);

        http_server = new ServerChannelLayer(serverWorkHandler, ServerChannelLayer.HTTP_TCP_PORT, serverClientId);
        https_server = new ServerChannelLayer(serverWorkHandler, ServerChannelLayer.HTTPS_TCP_PORT,
                serverClientId + 1, true);
    }

    public WebServer(int workerThreads, int listenPort, int serverClientId) {
        this.serverClientId = serverClientId;
        memoryManager = new MemoryManager();

        /*
         ** The queueSize is set to 2 to insure that the system runs out of connections and can be tested for
         **   the out of connections handling.
         */
        serverWorkHandler = new ServerLoadBalancer(2, workerThreads, memoryManager,
                (serverClientId * 100), digestThreadPool);

        http_server = new ServerChannelLayer(serverWorkHandler, listenPort, serverClientId);
        https_server = new ServerChannelLayer(serverWorkHandler, listenPort + 443,
                serverClientId + 1, true);
    }

    public void start() {
        serverWorkHandler.start();
        http_server.start();
        https_server.start();
        digestThreadPool.start();
    }

    public void stop() {
        serverWorkHandler.stop();
        http_server.stop();
        https_server.stop();
        digestThreadPool.stop();

        /*
         ** Verify that the MemoryManger has all of its memory back in the free pools
         */
        if (memoryManager.verifyMemoryPools("ServerChannelLayer")) {
            System.out.println("ServerChannelLayer[" + (serverClientId * 100) + "] Memory Verification All Passed");
        }
    }

}

