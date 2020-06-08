package com.webutils.webserver.manual;

import com.webutils.webserver.common.DeleteChunksParams;
import com.webutils.webserver.http.AllocateChunksResponseContent;
import com.webutils.webserver.http.DeleteChunksResponseParser;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteChunksSimple {

    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientServiceRequest cli;

    private final int eventThreadId;

    private final MemoryManager cliMemoryManager;

    DeleteChunksSimple(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount,
                          final List<ServerIdentifier> servers) {

        this.cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        this.eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        DeleteChunksParams params = new DeleteChunksParams(servers);
        params.setOpcClientRequestId("DeleteChunksSimple-6-8-2020.01");

        DeleteChunksResponseParser parser = new DeleteChunksResponseParser();
        cli = new ClientServiceRequest(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, parser, testCount);

    }

    public void execute() {
        cli.execute();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}
