package com.webutils.webserver.manual;

import com.webutils.webserver.common.GetObjectParams;
import com.webutils.webserver.common.Md5Digest;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.mysql.TestLocalDbInfo;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class GetObjectSimple {

    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_TEST_GET_OBJECT_SIMPLE;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientInterface cli;

    GetObjectSimple(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        DbSetup dbSetup = new TestLocalDbInfo(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, dbSetup);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        GetObjectParams params = new GetObjectParams("Namespace-xyz-987", "CreateBucket_Simple",
                "5e223890-ea13-11e9-851d-234132e0fb02");

        cli = new ClientInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, "testObjectFile", testCount);
    }

    public void execute() {
        cli.execute();

        cliClient.stop();
    }
}
