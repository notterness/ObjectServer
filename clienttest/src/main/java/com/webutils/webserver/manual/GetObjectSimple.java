package com.webutils.webserver.manual;

import com.webutils.webserver.common.GetObjectParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class GetObjectSimple {

    private static final Logger LOG = LoggerFactory.getLogger(GetObjectSimple.class);

    static WebServerFlavor flavor = WebServerFlavor.CLI_CLIENT;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientGetInterface cli;

    GetObjectSimple(final InetAddress serverIpAddr, final int serverTcpPort, final String accessToken,
                    AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        GetObjectParams params = new GetObjectParams("Namespace-xyz-987", "CreateBucket_Simple",
                "TestObject-1234-abcd", "testObjectFile", accessToken);
        params.setOpcClientRequestId("GetObjectSimple-5-12-2020.01");

        LOG.info("ClientTest - " + params.getOpcClientRequestId());
        System.out.println("\nSTARTING ClientTest - " + params.getOpcClientRequestId());

        cli = new ClientGetInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort, params, testCount);
    }

    public void execute() {
        cli.execute();

        int eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }
}
