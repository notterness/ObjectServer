package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.server.ServerChannelLayer;

import java.util.concurrent.atomic.AtomicInteger;

public class ServerTest implements Runnable {

    private int serverConnId;
    private boolean exitThread;

    private AtomicInteger serverCount;

    private Thread serverThread;

    public ServerTest(final int serverId, AtomicInteger threadCount) {
        serverConnId = serverId;
        exitThread = false;

        serverCount = threadCount;
        serverCount.incrementAndGet();
    }

    public void start() {
        serverThread = new Thread(this);
        serverThread.start();
    }

    public void stop() {
        exitThread = true;
    }

    public void run() {
        int tcpPort = ServerChannelLayer.baseTcpPort + serverConnId;

        System.out.println("ServerTest serverConnId: " + serverConnId);

        ServerChannelLayer server = new ServerChannelLayer(1, tcpPort, serverConnId);
        server.start();

        while (!exitThread) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                break;
            }
        }

        serverCount.decrementAndGet();
    }
}
