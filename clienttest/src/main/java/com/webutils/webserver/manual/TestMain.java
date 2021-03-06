package com.webutils.webserver.manual;

import com.webutils.accountmgr.requestcontext.AccountMgrContextPool;
import com.webutils.chunkmgr.requestcontext.ChunkAllocContextPool;
import com.webutils.objectserver.requestcontext.ObjectServerContextPool;
import com.webutils.storageserver.requestcontext.StorageServerContextPool;
import com.webutils.webserver.http.AllocateChunksResponseContent;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.TenancyUserHttpRequestInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.*;
import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.kubernetes.KubernetesInfo;
import com.webutils.webserver.niosockets.NioServerHandler;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpField;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// Server class
public class TestMain {
    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    public static void main(String[] args) {

        final String CLOSE_CONNECTION_AFTER_HEADER = "DisconnectAfterHeader";

        ServerIdentifierTableMgr serverTableMgr;

        final int OBJECT_SERVER_TCP_PORT = 5001;
        final int CHUNK_MGR_SERVICE_TCP_PORT = 5002;
        final int ACCOUNT_MGR_SERVICE_TCP_PORT = 5003;

        final String ACCOUNT_MGR_SERVICE = "account-mgr-service";

        final int NUMBER_TEST_STORAGE_SERVERS = 4;
        final int STORAGE_SERVER_BASE_ID_OFFSET = 100;

        int baseTcpPort = ServersDb.STORAGE_SERVER_TCP_PORT;

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
             **   the Object Server, Chunk Alloc Server and the Storage Servers.
             */
            serverTableMgr = new LocalServersMgr(flavor);
        }

        CreateObjectStorageTables objectStorageDbSetup = new CreateObjectStorageTables(flavor);
        AccessControlDb accessControlDb = new AccessControlDb(flavor);

        /*
        ** For test purposes, this is the place to drop all of the databases and allow the system to start in a
        **   clean state. The three databases are:
        **     ServiceServersDb - Used to keep track of the servers to provide a DNS type lookup of IP addresses and
        **        TCP Ports. It is also used by the Chunk Manager Service to manage the chunks that are used to store
        **        object data on the Storage Servers.
        **     ObjectStorageDb - This is the database that keeps track of all the information related the objects
        **        being managed by the client. This database is owned by the Object Server service.
        **     AccessControlDb - This is the database that keeps track of the customer tenancies and their users. It
        **        provides the authentication service. FIXME: Move this into its own service.
         */
        serverTableMgr.dropDatabase();
        objectStorageDbSetup.dropDatabase();
        accessControlDb.dropDatabase();

        /*
        ** The following builds a default configuration. It sets up the different databases if they are not already
        **   present. It then populates some initial information to allow the tests to run.
         */
        serverTableMgr.checkAndSetupServices();
        objectStorageDbSetup.checkAndSetupObjectStorageDb();
        accessControlDb.checkAndSetupAccessControls();

        /*
         ** Create the Administrator Tenancy to use for the test cases
         */
        String adminCustomerName = "Administrator Tenancy";
        String adminTenancyName = "AdministratorTenancy-Global";

        TenancyTableMgr tenancyMgr = new TenancyTableMgr(flavor);
        tenancyMgr.createTenancyEntry(adminCustomerName, adminTenancyName, "admin-passphrase");
        int tenancyId = tenancyMgr.getTenancyId(adminCustomerName, adminTenancyName);

        /*
        ** Create the user to send the Health Check commands
         */
        String adminName = "Administrator";
        HttpRequestInfo httpHealthCheckInfo = new TenancyUserHttpRequestInfo();
        HttpField adminTenancyField = new HttpField(HttpInfo.TENANCY_NAME, adminTenancyName);
        httpHealthCheckInfo.addHeaderValue(adminTenancyField);
        HttpField adminCustomerField = new HttpField(HttpInfo.CUSTOMER_NAME, adminCustomerName);
        httpHealthCheckInfo.addHeaderValue(adminCustomerField);

        String healthCheckUserName = "HealthCheckAdmin@test.com";
        String healthCheckUserPassword = "HealthCheckAdmin_password";
        HttpField healthCheckUserField = new HttpField(HttpInfo.USER_NAME, healthCheckUserName);
        httpHealthCheckInfo.addHeaderValue(healthCheckUserField);
        HttpField healthCheckPasswordField = new HttpField(HttpInfo.USER_PASSWORD, healthCheckUserPassword);
        httpHealthCheckInfo.addHeaderValue(healthCheckPasswordField);

        UserTableMgr userMgr = new UserTableMgr(flavor);
        userMgr.createTenancyUser(httpHealthCheckInfo);

        String healthCheckAccessToken = userMgr.getAccessToken(httpHealthCheckInfo);
        System.out.println("HealthCheck accessToken: " + healthCheckAccessToken);

        /*
        ** Create the Tenancy to use for the test cases
         */
        String customerName = "testCustomer";
        String tenancyName = "Tenancy-12345-abcde";

        tenancyMgr.createTenancyEntry(customerName, tenancyName, "test-passphrase");
        tenancyId = tenancyMgr.getTenancyId(customerName, tenancyName);

        /*
        ** Create a test user
         */
        HttpRequestInfo httpInfo = new TenancyUserHttpRequestInfo();
        HttpField tenancyField = new HttpField(HttpInfo.TENANCY_NAME, tenancyName);
        httpInfo.addHeaderValue(tenancyField);
        HttpField customerField = new HttpField(HttpInfo.CUSTOMER_NAME, customerName);
        httpInfo.addHeaderValue(customerField);

        String userName = "test_user@test.com";
        String userPassword = "test_password";
        HttpField userField = new HttpField(HttpInfo.USER_NAME, userName);
        httpInfo.addHeaderValue(userField);
        HttpField passwordField = new HttpField(HttpInfo.USER_PASSWORD, userPassword);
        httpInfo.addHeaderValue(passwordField);

        userMgr.createTenancyUser(httpInfo);

        String accessToken = userMgr.getAccessToken(httpInfo);
        System.out.println("accessToken: " + accessToken);

        int id = userMgr.getTenancyFromAccessToken(accessToken);
        System.out.println("tenancyId: " + tenancyId + " retrieved id: " + id);

        String namespace = "Namespace-xyz-987";
        NamespaceTableMgr namespaceMgr = new NamespaceTableMgr(flavor);
        namespaceMgr.createNamespaceEntry(namespace, tenancyId, "Noel-MAC");
        String namespaceUID = namespaceMgr.getNamespaceUID(namespace, tenancyId);

        /*
        ** Debug stuff to look at Kubernetes Pod information
         */
        String kubernetesPodIp = null;

        if (flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS) {
            KubernetesInfo kubeInfo = new KubernetesInfo(flavor);
            try {
                kubernetesPodIp = kubeInfo.getExternalK8Ip();
            } catch (IOException io_ex) {
                System.out.println("IOException: " + io_ex.getMessage());
            }
        } else if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
            /*
            ** This is only here to allow the Kubernetes API to be more easily debugged. It expects the Kubernetes POD
            **   to already be up and running for it to work. In general, it can simply be commented out as the code
            **   is not needed to run the tests in the IntelliJ environment.
             */
            /*
            KubernetesInfo kubeInfo = new KubernetesInfo(flavor);
            try {
                kubernetesPodIp = kubeInfo.getExternalKubeIp();
            } catch (IOException io_ex) {
                System.out.println("IOException: " + io_ex.getMessage());
            }
            */
        }

        AtomicInteger threadCount = new AtomicInteger(1);

        NioServerHandler nioServer;
        NioServerHandler chunkAllocNioServer;
        NioServerHandler accountMgrNioServer;
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
            ** Setup the Object Server
             */
            MemoryManager objectServerMemoryManager = new MemoryManager(flavor);
            ObjectServerContextPool objectRequestContextPool = new ObjectServerContextPool(flavor, objectServerMemoryManager, serverTableMgr);

            nioServer = new NioServerHandler(OBJECT_SERVER_TCP_PORT, NioServerHandler.OBJECT_SERVER_BASE_ID, objectRequestContextPool);
            nioServer.start();

            /*
             ** Setup the Chunk Mgr (this is used to manage the Storage Servers and the Chunk allocation/deallocation).
             */
            MemoryManager chunkAllocMemoryManager = new MemoryManager(flavor);
            ChunkAllocContextPool chunkAllocContextPool = new ChunkAllocContextPool(flavor, chunkAllocMemoryManager,
                    serverTableMgr);

            chunkAllocNioServer = new NioServerHandler(CHUNK_MGR_SERVICE_TCP_PORT, NioServerHandler.CHUNK_ALLOC_BASE_ID,
                    chunkAllocContextPool);
            chunkAllocNioServer.start();

            /*
             ** Setup the Account Manager (this is used to manage customer Tenancies and users).
             */
            MemoryManager AccountMgrMemoryManager = new MemoryManager(flavor);
            AccountMgrContextPool accountMgrContextPool = new AccountMgrContextPool(flavor, AccountMgrMemoryManager, serverTableMgr);

            accountMgrNioServer = new NioServerHandler(ACCOUNT_MGR_SERVICE_TCP_PORT, NioServerHandler.ACCOUNT_MGR_ALLOC_BASE_ID,
                    accountMgrContextPool);
            accountMgrNioServer.start();

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
            /*
            ** Initialize these to null to make sure they are not accessed in a bad code path.
             */
            nioServer = null;
            chunkAllocNioServer = null;
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

                KubernetesInfo k8Info = new KubernetesInfo(flavor);

                Map<String, Integer> storageServersInfo = new Hashtable<>();
                try {
                    k8Info.getStorageServerPorts(storageServersInfo);
                } catch (IOException io_ex) {
                    System.out.println("Unable to obtain Storage Server NodePorts - IOException: " + io_ex.getMessage());
                }

            } catch (UnknownHostException ex) {
                System.out.println("Unknown LocalHost");
                serverIpAddr = InetAddress.getLoopbackAddress();
            }
        }

        /*
        ** Validate the Health Check for the Object Server
         */
        SendHealthCheck healthCheck = new SendHealthCheck(serverIpAddr, OBJECT_SERVER_TCP_PORT, healthCheckAccessToken,
                threadCount);
        healthCheck.execute();

        /*
         ** Validate the Health Check for the ChunkMgr Service
         */
        SendHealthCheck chunkMgrHealthCheck = new SendHealthCheck(serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT,
                healthCheckAccessToken, threadCount);
        chunkMgrHealthCheck.execute();

        /*
         ** Validate the Health Check for the AccountMgr Service
         */
        SendHealthCheck accountMgrHealthCheck = new SendHealthCheck(serverIpAddr, ACCOUNT_MGR_SERVICE_TCP_PORT,
                healthCheckAccessToken, threadCount);
        accountMgrHealthCheck.execute();

        waitForTestsToComplete(threadCount);

        /*
        ** Create an extra Storage Server and set the address for the Chunk Manager, Object Server and Account Manager
        **   services so that they can be accessed.
        **
        ** NOTE: The serverIpAddr and the serverTcpPort (in this case CHUNK_MGR_SERVICE_TCP_PORT) point to the Chunk
        **   Manager Service. This is the service that keeps track of the different services in the database (more or
        **   less performs the role of a DHCP lookup).
         */
        PostCreateServer createServer = new PostCreateServer(baseTcpPort + 3, serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT, threadCount);
        createServer.execute();

        CreateService createChunkMgrService = new CreateService("chunk-mgr-service", CHUNK_MGR_SERVICE_TCP_PORT,
                serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT, threadCount);
        createChunkMgrService.execute();

        CreateService createObjectServerService = new CreateService("object-server", OBJECT_SERVER_TCP_PORT,
                serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT, threadCount);
        createObjectServerService.execute();

        CreateService createAccountMgrService = new CreateService(ACCOUNT_MGR_SERVICE, ACCOUNT_MGR_SERVICE_TCP_PORT,
                serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT, threadCount);
        createAccountMgrService.execute();

        waitForTestsToComplete(threadCount);

        /*
         ** Create a test Tenancy to validate the POST Tenancy method works with the Account Manager service
         */
        final String testCustomerName = "testCustomer-1";
        final String testTenancyName = "Tenancy-Test-Post";
        final String testTenancyPassphrase = "test-passphrase";

        PostTenancySimple postTenancy = new PostTenancySimple(testCustomerName, testTenancyName, testTenancyPassphrase,
                serverIpAddr, ACCOUNT_MGR_SERVICE_TCP_PORT, threadCount);
        postTenancy.execute();

        /*
         ** Create a test user to validate the POST User method works with the Account Manager service
         */
        final String testUser_1 = "TestUser1@test.com";
        final String testUser_1_Password = "testpassword";
        final int testUser_1_Permissions = 0x20;

        PostUserSimple postUser = new PostUserSimple(testCustomerName, testTenancyName, testUser_1, testUser_1_Password,
                testUser_1_Permissions, serverIpAddr, ACCOUNT_MGR_SERVICE_TCP_PORT, threadCount);
        postUser.execute();

        waitForTestsToComplete(threadCount);

        /*
         ** Obtain the access token from the Account Manager Service for the test user.
         */
        GetAccountMgrServiceInfo getAccountMgrInfo = new GetAccountMgrServiceInfo(serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT, threadCount);
        getAccountMgrInfo.execute();

        GetAccessTokenSimple obtainToken = new GetAccessTokenSimple(serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT,
                ACCOUNT_MGR_SERVICE, testCustomerName, testTenancyName, testUser_1, testUser_1_Password, threadCount);
        obtainToken.execute();

        waitForTestsToComplete(threadCount);

        /*
        ** Setup the infrastructure for the older type of tests
         */
        MemoryManager clientTestMemoryManager = new MemoryManager(flavor);
        ClientContextPool clientContextPool = new ClientContextPool(flavor, clientTestMemoryManager, serverTableMgr);
        NioTestClient testClient = new NioTestClient(clientContextPool);
        testClient.start();

        String bucketName = "CreateBucket_Simple";
        PostBucketSimple postBucket = new PostBucketSimple(namespace, bucketName, serverIpAddr, OBJECT_SERVER_TCP_PORT,
                accessToken, threadCount);
        postBucket.execute();

        PutObjectValidMd5 checkMd5 = new PutObjectValidMd5(namespace, bucketName, serverIpAddr, OBJECT_SERVER_TCP_PORT,
                accessToken, threadCount);
        checkMd5.execute();

        PutObjectBadMd5 badMd5 = new PutObjectBadMd5(namespace, bucketName, serverIpAddr, OBJECT_SERVER_TCP_PORT,
                accessToken, threadCount);
        badMd5.execute();

        waitForTestsToComplete(threadCount);

        //AllocateChunksSimple allocateChunks = new AllocateChunksSimple(serverIpAddr, CHUNK_ALLOC_SERVER_TCP_PORT, threadCount);
        //allocateChunks.execute();

        /*
        ** The AllocateChunksSimple2 and DeleteChunksSimple are sent directly to the Chunk Manager Service to validate
        **   the AllocateChunks and DeleteChunks methods work correctly.
         */
        AllocateChunksResponseContent contentParser = new AllocateChunksResponseContent();
        AllocateChunksSimple2 allocateChunks = new AllocateChunksSimple2(serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT,
                threadCount, contentParser);
        allocateChunks.execute();

        List<ServerIdentifier> servers = new LinkedList<>();
        contentParser.extractAllocations(servers, 0);

        if (servers.size() > 0) {
            /*
            ** Delete a single chunk from the Storage Server. This is not actually a good thing to do in real life, but
            **   for test purposes it allows the interface to be validated.
             */
            StorageServerChunkDeleteSimple storageServerDeleteChunk = new StorageServerChunkDeleteSimple(serverIpAddr,
                    baseTcpPort, threadCount, servers.get(0));
            storageServerDeleteChunk.execute();
        }

        DeleteChunksSimple deleteChunks = new DeleteChunksSimple(serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT, threadCount, servers);
        deleteChunks.execute();

        servers.clear();
        contentParser.cleanup();

        /*
        ** Read an object from the Object Server, save it to a file. Then upload that file as a new object. And
        **   finally, delete the object.
         */
        GetObjectSimple getObjectSimple = new GetObjectSimple(serverIpAddr, OBJECT_SERVER_TCP_PORT, accessToken,
                "GetObjectSimple-5-12-2020.01", threadCount);
        getObjectSimple.execute();

        PutObjectSimple putObjectSimple = new PutObjectSimple(serverIpAddr, OBJECT_SERVER_TCP_PORT, accessToken, threadCount);
        putObjectSimple.execute();


        ListObjectsSimple listObjectsSimple = new ListObjectsSimple(serverIpAddr, OBJECT_SERVER_TCP_PORT, accessToken, threadCount);
        listObjectsSimple.execute();

        ListAllocatedChunks listChunks = new ListAllocatedChunks(serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT, threadCount);
        listChunks.execute();

        ListServersAll listServersAll = new ListServersAll(serverIpAddr, CHUNK_MGR_SERVICE_TCP_PORT, threadCount);
        listServersAll.execute();

        /*
        ** Delete all of the objects created as part of this test suite. Since there are two objects contained within
        **   the "CreateBucket_Simple" Bucket, by trying to delete the Bucket before both objects are deleted will
        **   return an error.
        ** After the second Object is deleted, then the Delete Bucket will succeed.
         */
        DeleteObjectSimple deleteObject1 = new DeleteObjectSimple(serverIpAddr, OBJECT_SERVER_TCP_PORT,
                "TestObject_1", accessToken, threadCount);
        deleteObject1.execute();

        DeleteBucketSimple deleteBucket1 = new DeleteBucketSimple(serverIpAddr, OBJECT_SERVER_TCP_PORT,
                accessToken, threadCount);
        deleteBucket1.execute();

        DeleteObjectSimple deleteObject2 = new DeleteObjectSimple(serverIpAddr, OBJECT_SERVER_TCP_PORT,
                "TestObject-1234-abcd", accessToken, threadCount);
        deleteObject2.execute();

        DeleteBucketSimple deleteBucket2 = new DeleteBucketSimple(serverIpAddr, OBJECT_SERVER_TCP_PORT,
                accessToken, threadCount);
        deleteBucket2.execute();

        GetObjectBadBucket getObjectBadBucket = new GetObjectBadBucket(serverIpAddr, OBJECT_SERVER_TCP_PORT, accessToken, threadCount);
        getObjectBadBucket.execute();

        GetObjectBadNamespace getObjectBadNamespace = new GetObjectBadNamespace(serverIpAddr, OBJECT_SERVER_TCP_PORT, accessToken, threadCount);
        getObjectBadNamespace.execute();

        /*
        ** This attempts to perform the GetObject operation, but uses a bogus accessToken to test out the handling.
         */
        GetObjectSimple getObjectInvalidToken = new GetObjectSimple(serverIpAddr, OBJECT_SERVER_TCP_PORT, "BogusToken",
                "GetObjectInvalidToken-6-19-2020.01", threadCount);
        getObjectInvalidToken.execute();

        PutObjectBadBucket putObjectBadBucket = new PutObjectBadBucket(serverIpAddr, OBJECT_SERVER_TCP_PORT, accessToken, threadCount);
        putObjectBadBucket.execute();

        PutObjectBadNamespace putObjectBadNamespace = new PutObjectBadNamespace(serverIpAddr, OBJECT_SERVER_TCP_PORT, accessToken, threadCount);
        putObjectBadNamespace.execute();

        /*
        ** Uncomment out the following two lines to let TestMain just act as a server. It can then be used to
        **   handle requests from an external tool or command line. It will remain stuck in the
        **   waitForTestsToComplete().
         */
        waitForTestsToComplete(threadCount);

        /*
        TestClient client = null;

        ClientTest client_checkMd5 = new ClientTest_CheckMd5("CheckMd5", testClient, OBJECT_SERVER_TCP_PORT, threadCount);
        client_checkMd5.start();

        ClientTest client_badMd5 = new ClientTest_BadMd5("BadMd5", testClient, OBJECT_SERVER_TCP_PORT, threadCount);
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
            chunkAllocNioServer.stop();
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


