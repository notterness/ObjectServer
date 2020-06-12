package com.webutils.webserver.manual;

import com.webutils.webserver.common.AllocateChunksParams;
import com.webutils.webserver.common.ClientTestAllocateChunksParams;
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

    static WebServerFlavor flavor = WebServerFlavor.CLI_CLIENT;

    private static final int CHUNK_NUMBER = 0;
    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientServiceRequest cli;

    private final AllocateChunksResponseContent contentParser;

    AllocateChunksSimple2(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount,
                          final AllocateChunksResponseContent contentParser) {

        this.contentParser = contentParser;

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        AllocateChunksParams params = new ClientTestAllocateChunksParams(StorageTierEnum.STANDARD_TIER, CHUNK_NUMBER);
        params.setOpcClientRequestId("AllocateChunksSimple2-5-29-2020.01");

        cli = new ClientServiceRequest(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort, params, contentParser, testCount);
    }

    public void execute() {
        cli.execute();

        /*
         ** Now delete the chunks that were allocated
         */
        List<ServerIdentifier> servers = new LinkedList<>();
        contentParser.extractAllocations(servers, CHUNK_NUMBER);

        int eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}
