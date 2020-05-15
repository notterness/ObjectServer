package com.webutils.webserver.manual;

import com.webutils.webserver.common.DeleteObjectParams;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.mysql.TestLocalDbInfo;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteObjectSimple {

    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;

    private final ClientCommandInterface cli;
    private final int eventThreadId;

    DeleteObjectSimple(final InetAddress serverIpAddr, final int serverTcpPort, AtomicInteger testCount) {

        MemoryManager cliMemoryManager = new MemoryManager(flavor);

        DbSetup dbSetup = new TestLocalDbInfo(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, dbSetup);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        this.eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        DeleteObjectParams params = new DeleteObjectParams("Namespace-xyz-987", "CreateBucket_Simple",
                "TestObject_1", "testObjectFile");
        params.setOpcClientRequestId("DeleteObjectSimple-5-13-2020.01");

        cli = new ClientCommandInterface(cliClient, cliMemoryManager, serverIpAddr, serverTcpPort,
                params, testCount);
    }

    public void execute() {
        cli.execute();

        cliClient.stop();

        clientContextPool.stop(eventThreadId);
    }

}
