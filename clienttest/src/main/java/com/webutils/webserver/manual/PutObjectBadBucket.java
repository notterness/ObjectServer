package com.webutils.webserver.manual;

import com.webutils.webserver.common.PutObjectParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.mysql.TestLocalDbInfo;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class PutObjectBadBucket {
    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientPutInterface cli;

    private final int eventThreadId;

    PutObjectBadBucket(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        DbSetup dbSetup = new TestLocalDbInfo(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, dbSetup);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        this.eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        PutObjectParams params = new PutObjectParams("Namespace-xyz-987", "BadBucketName",
                "TestObject_1", "/Users/notterness/WebServer/webserver/logs/" + "testObjectFile");
        params.setOpcClientRequestId("PutObjectBadBucket-5-12-2020.01");

        cli = new ClientPutInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, testCount);
    }

    public void execute() {
        cli.execute();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}
