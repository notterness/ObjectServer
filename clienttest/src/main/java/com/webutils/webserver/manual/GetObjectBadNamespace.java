package com.webutils.webserver.manual;

import com.webutils.webserver.common.GetObjectParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class GetObjectBadNamespace {

    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientGetInterface cli;
    private final int eventThreadId;

    GetObjectBadNamespace(final InetAddress serverIpAddr, final int serverTcpPort, final String accessToken,
                          AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        this.eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        GetObjectParams params = new GetObjectParams("BadNamespaceName", "CreateBucket_Simple",
                "5e223890-ea13-11e9-851d-234132e0fb02", "testObjectFile", accessToken);
        params.setOpcClientRequestId("GetObjectSimple-5-12-2020.01");

        cli = new ClientGetInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, testCount);
    }

    public void execute() {
        cli.execute();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }
}
