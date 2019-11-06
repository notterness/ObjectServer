package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.server.ServerChannelLayer;

import java.util.concurrent.atomic.AtomicInteger;

public class ServerTest implements Runnable {

    private int serverConnId;
    private boolean exitThread;

    private AtomicInteger serverCount;

    private Thread serverThread;

    ServerTest(final int serverId, AtomicInteger threadCount) {
        serverConnId = serverId;
        exitThread = false;

        serverCount = threadCount;
        serverCount.incrementAndGet();
    }

    void start() {
        serverThread = new Thread(this);
        serverThread.start();
    }

    void stop() {
        exitThread = true;
        try {
            serverThread.join(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("serverThread.join() failed: " + int_ex.getMessage());
        }
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
