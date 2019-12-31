package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.NioTestClient;
import com.oracle.athena.webserver.niosockets.NioServerHandler;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;

import java.util.concurrent.atomic.AtomicInteger;

// Server class
public class TestMain {
    public static void main(String[] args) {
        final int serverTcpPort = 5001;
        final int storageServerTcpPort = 5010;

        AtomicInteger threadCount = new AtomicInteger(1);

        //TestEncryptBuffer testEncryptBuffer = new TestEncryptBuffer();
        //testEncryptBuffer.execute();

        //TestHttpParser testHttpParser = new TestHttpParser("TestHttpParser");
        //testHttpParser.execute();

        NioServerHandler nioServer = new NioServerHandler(WebServerFlavor.INTEGRATION_TESTS, serverTcpPort, 1000);
        nioServer.start();

        NioServerHandler nioStorageServer = new NioServerHandler(WebServerFlavor.INTEGRATION_TESTS, storageServerTcpPort, 1000);
        nioStorageServer.start();

        NioTestClient testClient = new NioTestClient(2000);
        testClient.start();

        TestChunkWrite testChunkWrite = new TestChunkWrite(testClient, storageServerTcpPort, threadCount);
        testChunkWrite.execute();

        //ClientTest client_1 = new ClientTest_2("ClientTest_2", testClient, serverTcpPort, threadCount);
        //client_1.execute();
        //ClientTest checkMd5 = new ClientTest_CheckMd5("CheckMd5", testClient, serverTcpPort, threadCount);
        //checkMd5.execute();

        /*
        ** Uncomment out the following two lines to let TestMain just act as a server. It can then be used to
        **   handle requests from an external tool or command line. It will remain stuck in the
        **   waitForTestsToComplete().
         */
        threadCount.incrementAndGet();
        waitForTestsToComplete(threadCount);

        /*
        TestClient client = null;

        ClientTest client_checkMd5 = new ClientTest_CheckMd5("CheckMd5", testClient, serverTcpPort, threadCount);
        client_checkMd5.start();

        ClientTest client_badMd5 = new ClientTest_BadMd5("BadMd5", testClient, serverTcpPort, threadCount);
        client_badMd5.start();

        String failedTestName = waitForTestsToComplete(threadCount, testClient);

        client_checkMd5.stop();
        client_badMd5.stop();

        if (failedTestName == null) {
            ClientTest client_invalidMd5 = new ClientTest_InvalidMd5Header("InvalidMd5Header", testClient, serverTcpPort, threadCount);
            client_invalidMd5.start();

            ClientTest client_missingObjectName = new ClientTest_MissingObjectName("MissingObjectName", testClient, serverTcpPort, threadCount);
            client_missingObjectName.start();

            failedTestName = waitForTestsToComplete(threadCount, testClient);

            client_invalidMd5.stop();
            client_missingObjectName.stop();
        }

        if (failedTestName == null) {

            ClientTest client_1 = new ClientTest_2("ClientTest_2", testClient, serverTcpPort, threadCount);
            client_1.start();

            ClientTest earlyClose = new ClientTest_EarlyClose("EarlyClose", testClient, serverTcpPort, threadCount);
            earlyClose.start();

            ClientTest slowHeaderSend = new ClientTest_SlowHeaderSend("SlowHeaderSend", testClient, serverTcpPort, threadCount);
            slowHeaderSend.start();

            ClientTest oneMbPut = new ClientTest_OneMbPut("OneMbPut", testClient, serverTcpPort, threadCount);
            oneMbPut.start();

            ClientTest outOfConnections = new ClientTest_OutOfConnections("OutOfConnections", testClient, serverTcpPort, threadCount);
            outOfConnections.start();

            System.out.println("Starting Tests");

            failedTestName = waitForTestsToComplete(threadCount, client);

            client_1.stop();
            earlyClose.stop();
            slowHeaderSend.stop();
            oneMbPut.stop();
            outOfConnections.stop();
        }
*/
        /*
        if (failedTestName == null) {
            ClientTest malformedRequest_1 = new ClientTest_MalformedRequest_1("MalformedRequest_1", testClient, serverTcpPort, threadCount);
            malformedRequest_1.start();

            ClientTest invalidContentLength = new ClientTest_InvalidContentLength("InvalidContentLength", testClient, serverTcpPort, threadCount);
            invalidContentLength.start();

            ClientTest noContentLength = new ClientTest_NoContentLength("NoContentLength", testClient, serverTcpPort, threadCount);
            noContentLength.start();

            failedTestName = waitForTestsToComplete(threadCount, testClient);

            malformedRequest_1.stop();
            invalidContentLength.stop();
            noContentLength.stop();
        }

        */

        System.out.println("Server shutting down");
    }

    /*
    ** This waits until the test have completed. When a test starts, it increments an atomic variable and
    **   decrements it when it completes. This allows the code to wait for a group of tests to complete
    **   prior to moving onto another set of tests or exiting.
     */
    static String waitForTestsToComplete(final AtomicInteger testRunningCount, final NioTestClient client) {
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

    /*
     ** This waits until the test have completed. When a test starts, it increments an atomic variable and
     **   decrements it when it completes. This allows the code to wait for a group of tests to complete
     **   prior to moving onto another set of tests or exiting.
     */
    static void waitForTestsToComplete(AtomicInteger testRunningCount) {
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
    }

}


