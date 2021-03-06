package com.webutils.webserver.manual;

import com.webutils.webserver.common.PostTenancyParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class PostTenancySimple {

    static WebServerFlavor flavor = WebServerFlavor.CLI_CLIENT;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientCommandInterface cli;

    PostTenancySimple(final String customer, final String tenancy, final String passphrase,
                      final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        PostTenancyParams params = new PostTenancyParams(tenancy, customer, passphrase);
        params.setOpcClientRequestId("PostTenancySimple-7-22-2020.01");

        cli = new ClientCommandInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort, params, testCount);
    }

    public void execute() {
        cli.execute();

        int eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }


}
