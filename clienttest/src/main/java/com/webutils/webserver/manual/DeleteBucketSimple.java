package com.webutils.webserver.manual;

import com.webutils.webserver.common.DeleteBucketParams;
import com.webutils.webserver.common.DeleteObjectParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteBucketSimple {

    static WebServerFlavor flavor = WebServerFlavor.CLI_CLIENT;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientServiceRequest cli;

    DeleteBucketSimple(final InetAddress serverIpAddr, final int serverTcpPort, final String accessToken,
                       AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        DeleteBucketParams params = new DeleteBucketParams("Namespace-xyz-987", "CreateBucket_Simple",
                accessToken);
        params.setOpcClientRequestId("DeleteBucketSimple-6-18-2020.01");

        cli = new ClientServiceRequest(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, null, testCount);
    }

    public void execute() {
        cli.execute();

        int eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}
