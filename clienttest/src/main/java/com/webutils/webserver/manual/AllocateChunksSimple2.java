package com.webutils.webserver.manual;

import com.webutils.webserver.common.AllocateChunksParams;
import com.webutils.webserver.http.AllocateChunksResponseContent;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class AllocateChunksSimple2 {

    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientServiceRequest cli;

    private final AllocateChunksResponseContent contentParser;

    private final int eventThreadId;

    AllocateChunksSimple2(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        this.eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        AllocateChunksParams params = new AllocateChunksParams(StorageTierEnum.STANDARD_TIER, 0);
        params.setOpcClientRequestId("AllocateChunksSimple2-5-29-2020.01");

        contentParser = new AllocateChunksResponseContent();

        cli = new ClientServiceRequest(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, contentParser, testCount);
    }

    public void execute() {
        cli.execute();

        List<ServerIdentifier> servers = new LinkedList<>();
        contentParser.extractAllocations(servers, 0);

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}