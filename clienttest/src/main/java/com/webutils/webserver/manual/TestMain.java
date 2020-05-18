package com.webutils.webserver.manual;

import com.google.common.io.BaseEncoding;
import com.webutils.objectserver.requestcontext.ObjectServerContextPool;
import com.webutils.storageserver.requestcontext.StorageServerContextPool;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.*;
import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.kubernetes.KubernetesInfo;
import com.webutils.webserver.niosockets.NioServerHandler;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

// Server class
public class TestMain {
    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    public static void main(String[] args) {

        final String CLOSE_CONNECTION_AFTER_HEADER = "DisconnectAfterHeader";

        String md5FromHeader = "RUyS5v1cKdbFcsRdO7NgfQ==";
        byte[] bytes = BaseEncoding.base64().decode(md5FromHeader);
        if (bytes.length != 16) {
            System.out.println("The value of the Content-MD5 header '" + md5FromHeader +
                    "' was not the correct length after base-64 decoding");
        } else {
            String encodeStr = BaseEncoding.base64().encode(bytes);

            boolean match = encodeStr.equals(md5FromHeader);
            System.out.println("initial " + md5FromHeader + " encode: " + encodeStr + " match: " + match);
        }

        ServerIdentifierTableMgr serverTableMgr;

        final int serverTcpPort = 5001;

        final int NUMBER_TEST_STORAGE_SERVERS = 3;
        final int STORAGE_SERVER_BASE_ID_OFFSET = 100;

        int baseTcpPort = ServersDb.storageServerTcpPort;

        if (args.length >= 1) {
            try {
                baseTcpPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException ex) {
                System.out.println("Passed in arg is not a valid integer - " + args[0]);
            }
        }

        /*
         ** Check if this is a Docker image
         */
        if (args.length == 2) {
            if (args[1].compareTo("docker") == 0) {
                flavor = WebServerFlavor.INTEGRATION_DOCKER_TESTS;
            } else if (args[1].compareTo("docker-test") == 0) {
                /*
                ** These are the tests run against the Storage Server and Object Server running in a different Kubernetes POD
                 */
                flavor = WebServerFlavor.INTEGRATION_KUBERNETES_TESTS;
            }

            serverTableMgr = new K8PodServersMgr(flavor);
        } else {
            /*
             ** The INTEGRATION_KUBERNETES_TESTS needs access to the database to obtain the IP address and Port for
             **   the Object Server and the StorageServers.
             */
            serverTableMgr = new LocalServersMgr(flavor);
        }

        serverTableMgr.checkAndSetupStorageServers();

        CreateObjectStorageTables objectStorageDbSetup = new CreateObjectStorageTables(flavor);
        objectStorageDbSetup.checkAndSetupObjectStorageDb();

        TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);
        tenancyMgr.createTenancyEntry("testCustomer", "Tenancy-12345-abcde");
        String tenancyUID = tenancyMgr.getTenancyUID("testCustomer", "Tenancy-12345-abcde");

        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        namespaceMgr.createNamespaceEntry("Namespace-xyz-987", tenancyUID, "Noel-MAC");
        String namespaceUID = namespaceMgr.getNamespaceUID("Namespace-xyz-987", tenancyUID);

        /*
        ** Debug stuff to look at Kubernetes Pod information
         */
        String kubernetesPodIp = null;

        if (flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS) {
            KubernetesInfo kubeInfo = new KubernetesInfo(flavor);
            try {
                kubernetesPodIp = kubeInfo.getExternalKubeIp();
            } catch (IOException io_ex) {
                System.out.println("IOException: " + io_ex.getMessage());
            }
        } else if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
            /*
            ** This is only here to allow the Kubernetes API to be more easily debugged. It expects the Kubernetes POD
            **   to already be up and running for it to work. In general, it can simply be commented out as the code
            **   is not needed to run the tests in the IntelliJ environment.
             */
            KubernetesInfo kubeInfo = new KubernetesInfo(flavor);
            try {
                kubernetesPodIp = kubeInfo.getExternalKubeIp();
            } catch (IOException io_ex) {
                System.out.println("IOException: " + io_ex.getMessage());
            }
        }

        AtomicInteger threadCount = new AtomicInteger(1);

        NioServerHandler nioServer;

        NioServerHandler[] nioStorageServer = new NioServerHandler[NUMBER_TEST_STORAGE_SERVERS];

        if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
            /*
            ** Test the Encrypt Buffer path that is used during the writing of an Object to the Storage Servers
             */
            //TestEncryptBuffer testEncryptBuffer = new TestEncryptBuffer();
            //testEncryptBuffer.execute();

            /*
             ** The HTTP Parser test is currently not working.
             */
            //TestHttpParser testHttpParser = new TestHttpParser("TestHttpParser");
            //testHttpParser.execute();
            //threadCount.incrementAndGet();
            //waitForTestsToComplete(threadCount);

            /*
            ** The Object Server needs to access the database to obtain the VON information
             */
            MemoryManager objectServerMemoryManager = new MemoryManager(flavor);
            ObjectServerContextPool objectRequestContextPool = new ObjectServerContextPool(flavor, objectServerMemoryManager, serverTableMgr);

            nioServer = new NioServerHandler(serverTcpPort, NioServerHandler.OBJECT_SERVER_BASE_ID, objectRequestContextPool);
            nioServer.start();

            /*
            ** Setup the Storage Servers
             */
            for (int i = 0; i < NUMBER_TEST_STORAGE_SERVERS; i++) {
                MemoryManager storageServersMemoryManager = new MemoryManager(flavor);
                StorageServerContextPool storageRequestContextPool = new StorageServerContextPool(flavor, storageServersMemoryManager, null);

                nioStorageServer[i] = new NioServerHandler(baseTcpPort + i,
                        (NioServerHandler.STORAGE_SERVER_BASE_ID + (i * STORAGE_SERVER_BASE_ID_OFFSET)), storageRequestContextPool);
                nioStorageServer[i].start();
            }
        } else {
            nioServer = null;
            for (int i = 0; i < NUMBER_TEST_STORAGE_SERVERS; i++) {
                nioStorageServer[i] = null;
            }
        }

        InetAddress addr = null;
        if (flavor == WebServerFlavor.INTEGRATION_DOCKER_TESTS) {
            try {
                addr = InetAddress.getByName("StorageServer");

            } catch (UnknownHostException ex) {
                System.out.println("Unknown host: StorageServer " + ex.getMessage());
            }
        } else if (flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS){
            if (kubernetesPodIp != null) {
                try {
                    System.out.println("Kubernetes POD IP: " + kubernetesPodIp);
                    addr = InetAddress.getByName(kubernetesPodIp);
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
            try {
                addr = InetAddress.getLocalHost();
            } catch (UnknownHostException ex) {
                System.out.println("Unknown LocalHost");
            }
            //addr = InetAddress.getLoopbackAddress();
        }

        if (addr != null) {
            /*
            ** First check with a valid Storage Server (this is the baseTcpPort)
             */
            //TestChunkWrite testChunkWrite = new TestChunkWrite(addr, baseTcpPort, threadCount, serversDb, null);
            //testChunkWrite.execute();

            /*
            ** Now check with an invalid Storage Server (there is nothing listening at the baseTcpPort + 20) so the connection
            **   to the Storage Server will fail.
             */
            //TestChunkWrite testChunkWrite_badStorageServer = new TestChunkWrite(addr, baseTcpPort + 20,
            //        threadCount, serversDb, null);
            //testChunkWrite_badStorageServer.execute();

            /*
             ** Now check with an invalid Storage Server (there is nothing listening at the baseTcpPort + 20) so the connection
             **   to the Storage Server will fail.
             */
            //TestChunkWrite testChunkWrite_disconnectAfterHeader = new TestChunkWrite(addr, baseTcpPort,
            //        threadCount, serversDb, CLOSE_CONNECTION_AFTER_HEADER);
            //testChunkWrite_disconnectAfterHeader.execute();
        } else {
            System.out.println("ERROR: addr for TestChunkWrite() null");
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
            /*
             ** flavor == WebServerFlavor.INTEGRATION_TESTS
             */
            try {
                serverIpAddr = InetAddress.getLocalHost();
            } catch (UnknownHostException ex) {
                System.out.println("Unknown LocalHost");
                serverIpAddr = InetAddress.getLoopbackAddress();
            }
        }

        MemoryManager clientTestMemoryManager = new MemoryManager(flavor);

        ClientContextPool clientContextPool = new ClientContextPool(flavor, clientTestMemoryManager, serverTableMgr);
        NioTestClient testClient = new NioTestClient(clientContextPool);
        testClient.start();

/*
        ClientTest client_CreateBucket_Simple = new ClientTest_CreateBucket_Simple("CreateBucket_Simple", testClient,
                serverIpAddr, serverTcpPort, threadCount);
        client_CreateBucket_Simple.execute();

        ClientTest checkMd5 = new ClientTest_CheckMd5("CheckMd5", testClient, serverIpAddr, serverTcpPort, threadCount);
        checkMd5.execute();

        waitForTestsToComplete(threadCount);
*/


        //DeleteObjectSimple deleteObject = new DeleteObjectSimple(serverIpAddr, serverTcpPort, threadCount);
        //deleteObject.execute();

        //GetObjectSimple getObjectSimple = new GetObjectSimple(serverIpAddr, serverTcpPort, threadCount);
        //getObjectSimple.execute();

        //ListObjectsSimple listObjectsSimple = new ListObjectsSimple(serverIpAddr, serverTcpPort, threadCount);
        //listObjectsSimple.execute();

        /*
        GetObjectBadBucket getObjectBadBucket = new GetObjectBadBucket(serverIpAddr, serverTcpPort, threadCount);
        getObjectBadBucket.execute();

        GetObjectBadNamespace getObjectBadNamespace = new GetObjectBadNamespace(serverIpAddr, serverTcpPort, threadCount);
        getObjectBadNamespace.execute();

        PutObjectBadBucket putObjectBadBucket = new PutObjectBadBucket(serverIpAddr, serverTcpPort, threadCount);
        putObjectBadBucket.execute();

        PutObjectBadNamespace putObjectBadNamespace = new PutObjectBadNamespace(serverIpAddr, serverTcpPort, threadCount);
        putObjectBadNamespace.execute();
*/
        PutObjectSimple putObjectSimple = new PutObjectSimple(serverIpAddr, serverTcpPort, threadCount);
        putObjectSimple.execute();

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

        if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
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


