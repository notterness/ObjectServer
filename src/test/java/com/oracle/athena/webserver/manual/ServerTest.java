package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.common.GlobalSystemPropertiesConfigurator;
import com.oracle.athena.webserver.server.ServerChannelLayer;
import com.oracle.athena.webserver.server.WebServer;
import com.oracle.pic.casper.common.config.ConfigAvailabilityDomain;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;

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
        System.out.println("ServerTest[" + serverConnId + "] stop() begin");

        exitThread = true;
        try {
            serverThread.join(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("serverThread.join() failed: " + int_ex.getMessage());
        }

        System.out.println("ServerTest[" + serverConnId + "] stop() done");
    }

    public void run() {
        int tcpPort = ServerChannelLayer.BASE_TCP_PORT + serverConnId;

        System.out.println("ServerTest[" + serverConnId + "] thread start");

        GlobalSystemPropertiesConfigurator.configure();
        final ConfigAvailabilityDomain ad =
                ConfigAvailabilityDomain.tryFromSystemProperty().orElse(ConfigAvailabilityDomain.GLOBAL);

        WebServer server = new WebServer(
                WebServerFlavor.INTEGRATION_TESTS, tcpPort, serverConnId, CasperConfig.create(ConfigRegion.LOCAL, ad));
        server.start();

        while (!exitThread) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                break;
            }
        }

        /*
        ** Stop the WebServer so it can cleanup and verify the resources
         */
        server.stop();

        System.out.println("ServerTest[" + serverConnId + "] thread exit");

        serverCount.decrementAndGet();
    }
}
