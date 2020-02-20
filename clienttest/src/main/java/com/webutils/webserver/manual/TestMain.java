package com.webutils.webserver.manual;

import com.webutils.webserver.client.NioTestClient;
import com.webutils.webserver.kubernetes.KubernetesInfo;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.NioServerHandler;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

// Server class
public class TestMain {
    public static void main(String[] args) {

        WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

        final int serverTcpPort = 5001;

        final int NUMBER_TEST_STORAGE_SERVERS = 3;
        final int STORAGE_SERVER_BASE_ID_OFFSET = 100;

        int baseTcpPort = DbSetup.storageServerTcpPort;
        boolean dockerImage = false;

        if (args.length >= 1) {
            try {
                baseTcpPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException ex) {
            }
        }

        /*
         ** Check if this is a Docker image
         */
        if (args.length == 2) {
            if (args[1].compareTo("docker") == 0) {
                flavor = WebServerFlavor.INTEGRATION_DOCKER_TESTS;
            } else if (args[1].compareTo("docker-test") == 0) {
                flavor = WebServerFlavor.INTEGRATION_KUBERNETES_TESTS;
            }
        }

        /*
        ** Debug stuff to look at Kubernetes Pod information
         */
        String kubernetesPodIp = null;

        if ((flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS) || (flavor == WebServerFlavor.INTEGRATION_TESTS)){
            KubernetesInfo kubeInfo = new KubernetesInfo(flavor);
            try {
                kubernetesPodIp = kubeInfo.getKubeInfo();
            } catch (IOException io_ex) {
                System.out.println("IOException: " + io_ex.getMessage());
            }
        }

        AtomicInteger threadCount = new AtomicInteger(1);

        NioServerHandler nioServer;
        NioServerHandler[] nioStorageServer = new NioServerHandler[NUMBER_TEST_STORAGE_SERVERS];

        if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
            TestEncryptBuffer testEncryptBuffer = new TestEncryptBuffer();
            testEncryptBuffer.execute();

            /*
             ** The HTTP Parser test is currently not working.
             */
            //TestHttpParser testHttpParser = new TestHttpParser("TestHttpParser");
            //testHttpParser.execute();
            //threadCount.incrementAndGet();
            //waitForTestsToComplete(threadCount);

            nioServer = new NioServerHandler(WebServerFlavor.INTEGRATION_OBJECT_SERVER_TEST, serverTcpPort, 1000);
            nioServer.start();

            for (int i = 0; i < NUMBER_TEST_STORAGE_SERVERS; i++) {
                nioStorageServer[i] = new NioServerHandler(WebServerFlavor.INTEGRATION_STORAGE_SERVER_TEST, baseTcpPort + i,
                        (2000 + (i * STORAGE_SERVER_BASE_ID_OFFSET)));
                nioStorageServer[i].start();
            }
        } else {
            nioServer = null;
            for (int i = 0; i < NUMBER_TEST_STORAGE_SERVERS; i++) {
                nioStorageServer[i] = null;
            }
        }

        NioTestClient testClient = new NioTestClient(3000);
        testClient.start();

        if (flavor == WebServerFlavor.INTEGRATION_DOCKER_TESTS) {
            try {
                InetAddress addr = InetAddress.getByName("StorageServer");

                TestChunkWrite testChunkWrite = new TestChunkWrite(testClient, addr, baseTcpPort, threadCount);
                testChunkWrite.execute();
            } catch (UnknownHostException ex) {
                System.out.println("Unknown host: StorageServer " + ex.getMessage());
            }
        } else if (flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS){
            if (kubernetesPodIp != null) {
                try {
                    System.out.println("Kubernetes POD IP: " + kubernetesPodIp);
                    InetAddress addr = InetAddress.getByName(kubernetesPodIp);
                    TestChunkWrite testChunkWrite = new TestChunkWrite(testClient, addr, baseTcpPort, threadCount);
                    testChunkWrite.execute();
                } catch (UnknownHostException ex) {
                    System.out.println("Kubernetes POD IP: " + kubernetesPodIp + " - " + ex.getMessage());
                }
            } else {
                System.out.println("Kubernetes POD IP null");
            }
        } else {
            /*
            ** flavor == WebServerFlavor.INTEGRATION_TESTS
             */
            InetAddress addr = InetAddress.getLoopbackAddress();
            TestChunkWrite testChunkWrite = new TestChunkWrite(testClient, addr, baseTcpPort, threadCount);
            testChunkWrite.execute();
        }

        InetAddress serverIpAddr = null;

        if (flavor == WebServerFlavor.INTEGRATION_DOCKER_TESTS) {
            try {
                serverIpAddr = InetAddress.getByName("ObjectServer");
            } catch (UnknownHostException ex) {
                System.out.println("Unknown host: ObjectServer " + ex.getMessage());
            }
        } else if (flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS){
            if (kubernetesPodIp != null) {
                try {
                    serverIpAddr = InetAddress.getByName(kubernetesPodIp);
                } catch (UnknownHostException ex) {
                    System.out.println("Kubernetes POD IP: " + kubernetesPodIp + " - " + ex.getMessage());
                }
            } else {
                System.out.println("Kubernetes POD IP null");
            }
        } else {
            serverIpAddr = InetAddress.getLoopbackAddress();
        }

        //ClientTest client_1 = new ClientTest_2("ClientTest_2", testClient, serverTcpPort, threadCount);
        //client_1.execute();
        ClientTest checkMd5 = new ClientTest_CheckMd5("CheckMd5", testClient, serverIpAddr, serverTcpPort, threadCount);
        checkMd5.execute();

        /*
        ** Uncomment out the following two lines to let TestMain just act as a server. It can then be used to
        **   handle requests from an external tool or command line. It will remain stuck in the
        **   waitForTestsToComplete().
         */
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

        if (!dockerImage) {
            nioServer.stop();
            for (int i = 0; i < NUMBER_TEST_STORAGE_SERVERS; i++) {
                nioStorageServer[i].stop();
            }
        }
        testClient.stop();

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
                System.out.println("\nwaitForTestsToComplete(1) count: " + count + "\n");

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
                System.out.println("\nwaitForTestsToComplete(2) count: " + count + "\n");
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

