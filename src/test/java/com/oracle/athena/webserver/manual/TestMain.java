package com.oracle.athena.webserver.manual;

import java.util.concurrent.atomic.AtomicInteger;

// Server class
public class TestMain
{
    public static void main(String[] args)
    {
        final int baseTcpPortOffset = 1;

        int count;

        AtomicInteger threadCount = new AtomicInteger(0);

        ServerTest server_1 = new ServerTest(baseTcpPortOffset, threadCount);
        server_1.start();

        /*
        ** The first parameter is the port offset the server for the client will be listening on. The
        **   second parameter is the port offset the client will be connecting to for writes.
        ** In this case the server that is created will be listening on (where the accept() takes place)
        **    (ServerChannelLayer.baseTcpPort + (baseTcpPortOffset + 1))
        **   and it will be writing to (who the connect() is to)
        **    (ServerChannelLayer.baseTcpPort + baseTcpPortOffset)
        */
        ClientTest client_1 = new ClientTest((baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
        client_1.start();

        System.out.println("Starting Server");

        // running infinite loop for getting
        // client request
        while (true)
        {
            count = threadCount.get();
            if (count != 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    break;
                }
            } else {
                break;
            }

        }

        System.out.println("Server shutting down");
    }

}


