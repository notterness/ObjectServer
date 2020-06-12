package com.webutils.webserver.manual;

import com.webutils.webserver.common.CreateBucketParams;
import com.webutils.webserver.common.ListObjectsParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class PostBucketSimple {

    static WebServerFlavor flavor = WebServerFlavor.CLI_CLIENT;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientCommandInterface cli;

    PostBucketSimple(final String tenancyName, final String namespace, final String bucketName, final InetAddress serverIpAddr,
                     final int serverTcpPort, final String accessToken, AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        CreateBucketParams params = new CreateBucketParams(tenancyName, namespace, bucketName, accessToken);
        params.setOpcClientRequestId("PostBucketsSimple-6-12-2020.01");

        cli = new ClientCommandInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort, params, testCount);
    }

    public void execute() {
        cli.execute();

        int eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}
