package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.TestClient;

import java.util.concurrent.atomic.AtomicInteger;

// Server class
public class TestMain {
    public static void main(String[] args) {
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
        // This sets up the server side of the connection

        TestClient client = new TestClient((baseTcpPortOffset + 1));
        client.start();

        ClientTest client_1 = new ClientTest_2(client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
        client_1.start();

        ClientTest client_2 = new ClientTest_EarlyClose(client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
        client_2.start();

        ClientTest client_3 = new ClientTest_SlowHeaderSend(client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
        client_3.start();

        ClientTest client_4 = new ClientTest_OneMbPut(client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
        client_4.start();

        ClientTest client_5 = new ClientTest_OutOfConnections(client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
        client_5.start();

        System.out.println("Starting Tests");

        // running infinite loop for getting
        // client request
        while (true) {
            count = threadCount.get();

            /*
            ** The TestClient() cannot finish until after all the tests have completed so there is always
            **   a count of 1 even after all the tests have completed.
             */
            if (count != 1) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    break;
                }
            } else {
                break;
            }
        }

        client_1.stop();
        client_2.stop();
        client_3.stop();
        client_4.stop();
        client_5.stop();

        System.out.println("Tests Completed");

        /* Stop the TestClient */
        client.stop();

        System.out.println("Server shutting down");

        server_1.stop();
    }

}


