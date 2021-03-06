package com.webutils.webserver.manual;

import com.webutils.webserver.common.PutObjectParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class PutObjectBadNamespace {

    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientPutInterface cli;

    private final int eventThreadId;

    PutObjectBadNamespace(final InetAddress serverIpAddr, final int serverTcpPort, final String accessToken,
                          final AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        this.eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        PutObjectParams params = new PutObjectParams("BadNamespaceName", "CreateBucket_Simple",
                "TestObject_1", "/Users/notterness/WebServer/webserver/logs/" + "testObjectFile",
                accessToken);
        params.setOpcClientRequestId("PutObjectBadNamespace-5-12-2020.01");

        cli = new ClientPutInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, testCount);
    }

    public void execute() {
        cli.execute();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }


}
