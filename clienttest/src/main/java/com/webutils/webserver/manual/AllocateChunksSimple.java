package com.webutils.webserver.manual;

import com.webutils.webserver.common.AllocateChunksParams;
import com.webutils.webserver.common.PutObjectParams;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class AllocateChunksSimple {

    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientCommandInterface cli;

    private final int eventThreadId;

    AllocateChunksSimple(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        this.eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        AllocateChunksParams params = new AllocateChunksParams(StorageTierEnum.STANDARD_TIER, 0);
        params.setOpcClientRequestId("AllocateChunksSimple-5-21-2020.01");

        cli = new ClientCommandInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, testCount);
    }

    public void execute() {
        cli.execute();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}
