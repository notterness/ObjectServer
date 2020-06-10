package com.webutils.webserver.manual;

import com.webutils.webserver.common.StorageServerChunkDeleteParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageServerChunkDeleteSimple {

    static WebServerFlavor flavor = WebServerFlavor.CLI_CLIENT;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientServiceRequest cli;

    StorageServerChunkDeleteSimple(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount,
                       final ServerIdentifier server) {

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        MemoryManager cliMemoryManager = new MemoryManager(flavor);
        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        StorageServerChunkDeleteParams params = new StorageServerChunkDeleteParams(server);
        params.setOpcClientRequestId("StorageServerChunkDeleteSimple-6-9-2020.01");

        /*
        ** The StorageServerChunkDelete method does not need a content parser since it will always return a
        **   "Content-Length" of 0.
         */
        cli = new ClientServiceRequest(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort, params, null, testCount);

    }

    public void execute() {
        cli.execute();

        int eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}
