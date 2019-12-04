package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.TestClient;

import java.util.concurrent.atomic.AtomicInteger;

// Server class
public class TestMain {
    public static void main(String[] args) {
        final int baseTcpPortOffset = 1;

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
        TestClient client = new TestClient((baseTcpPortOffset + 1));
        client.start();

        /*
        ** Uncomment out the following two lines to let TestMain just act as a server. It can then be used to
        **   handle requests from an external tool or command line. It will remain stuck in the
        **   waitForTestsToComplete().
         */
        //threadCount.incrementAndGet();
        //waitForTestsToComplete(threadCount, client);

        ClientTest client_checkMd5 = new ClientTest_CheckMd5("CheckMd5", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
        client_checkMd5.start();

        ClientTest client_badMd5 = new ClientTest_BadMd5("BadMd5", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
        client_badMd5.start();

        String failedTestName = waitForTestsToComplete(threadCount, client);

        client_checkMd5.stop();
        client_badMd5.stop();

        if (failedTestName == null) {
            //ClientTest client_invalidMd5 = new ClientTest_InvalidMd5Header("InvalidMd5Header", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            //client_invalidMd5.start();

            ClientTest client_missingObjectName = new ClientTest_MissingObjectName("MissingObjectName", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            client_missingObjectName.start();

            failedTestName = waitForTestsToComplete(threadCount, client);

            //client_invalidMd5.stop();
            client_missingObjectName.stop();
        }

        if (failedTestName == null) {

            ClientTest client_1 = new ClientTest_2("ClientTest_2", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            client_1.start();

            ClientTest earlyClose = new ClientTest_EarlyClose("EarlyClose", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            earlyClose.start();

            ClientTest slowHeaderSend = new ClientTest_SlowHeaderSend("SlowHeaderSend", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            slowHeaderSend.start();

            ClientTest oneMbPut = new ClientTest_OneMbPut("OneMbPut", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            oneMbPut.start();

            ClientTest outOfConnections = new ClientTest_OutOfConnections("OutOfConnections", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            outOfConnections.start();

            System.out.println("Starting Tests");

            failedTestName = waitForTestsToComplete(threadCount, client);

            client_1.stop();
            earlyClose.stop();
            slowHeaderSend.stop();
            oneMbPut.stop();
            outOfConnections.stop();
        }

        if (failedTestName == null) {
            /*
             ** Start next set of tests to validate the HTTP Parser handling for incorrect HTTP Requests
             */
            ClientTest malformedRequest_1 = new ClientTest_MalformedRequest_1("MalformedRequest_1", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            malformedRequest_1.start();

            ClientTest invalidContentLength = new ClientTest_InvalidContentLength("InvalidContentLength", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            invalidContentLength.start();

            ClientTest noContentLength = new ClientTest_NoContentLength("NoContentLength", client, (baseTcpPortOffset + 1), baseTcpPortOffset, threadCount);
            noContentLength.start();

            failedTestName = waitForTestsToComplete(threadCount, client);

            malformedRequest_1.stop();
            invalidContentLength.stop();
            noContentLength.stop();
        }

        /* Stop the TestClient */
        client.stop();

        System.out.println("Server shutting down");

        server_1.stop();
    }

    /*
    ** This waits until the test have completed. When a test starts, it increments an atomic variable and
    **   decrements it when it completes. This allows the code to wait for a group of tests to complete
    **   prior to moving onto another set of tests or exiting.
     */
    static String waitForTestsToComplete(AtomicInteger testRunningCount, TestClient client) {
        // running infinite loop for getting
        // client request
        while (true) {
            int count = testRunningCount.get();

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

        try {
            Thread.sleep(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("Wait after test run was interrupted");
        }

        String failedTestName = client.getFailedTestName();
        if (failedTestName == null) {
            System.out.println("\nData Transfer Tests Completed With No Failures\n");
        } else {
            System.out.println("FAILURE: At Least One Test Failed - first failed test " + failedTestName);
        }

        return failedTestName;
    }

}


