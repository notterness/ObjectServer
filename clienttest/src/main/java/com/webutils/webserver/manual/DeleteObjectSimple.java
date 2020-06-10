package com.webutils.webserver.manual;

import com.webutils.webserver.common.DeleteObjectParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteObjectSimple {

    static WebServerFlavor flavor = WebServerFlavor.CLI_CLIENT;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientServiceRequest cli;

    DeleteObjectSimple(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount, final String objectName) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        DeleteObjectParams params = new DeleteObjectParams("Namespace-xyz-987", "CreateBucket_Simple",
                objectName);
        params.setOpcClientRequestId("DeleteObjectSimple-5-13-2020.01-" + objectName);

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
