package com.webutils.webserver.manual;

import com.webutils.webserver.common.PutObjectMd5Params;
import com.webutils.webserver.http.PutObjectResponseParser;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class PutObjectBadMd5 {

    static WebServerFlavor flavor = WebServerFlavor.CLI_CLIENT;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientServiceRequestWithData cli;

    PutObjectBadMd5(final String namespace, final String bucketName, final InetAddress serverIpAddr,
                    final int serverTcpPort, final String accessToken, final AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        PutObjectResponseParser contentParser = new PutObjectResponseParser();

        /*
        ** Set the validMd5 boolean to false to force the Md5 digest that is generated to be invalid.
         */
        PutObjectMd5Params params = new PutObjectMd5Params(cliMemoryManager, namespace, bucketName,
                "TestObject-6789-efgh", false, accessToken);
        params.setOpcClientRequestId("PutObjectBadMd5-6-17-2020.01");

        cli = new ClientServiceRequestWithData(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort, params, contentParser, testCount);
    }

    public void execute() {
        cli.execute();

        int eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }


}
