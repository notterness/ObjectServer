package com.webutils.webserver.manual;

import com.webutils.webserver.common.GetAccessTokenParams;
import com.webutils.webserver.common.ListServersParams;
import com.webutils.webserver.http.ServiceListContentParser;
import com.webutils.webserver.http.StorageTierEnum;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.operations.ClientCommandSend;
import com.webutils.webserver.operations.SendRequestToService;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class GetAccessTokenSimple {

    static WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final ClientContextPool clientContextPool;
    private final NioCliClient cliClient;
    private final MemoryManager cliMemoryManager;

    private final ClientCommandInterface cli;
    private final int eventThreadId;

    private final ServiceListContentParser contentParser;

    /*
    ** The following four are required to obtain the accessToken
     */
    private final String tenancy;
    private final String customer;
    private final String user;
    private final String password;

    /*
    ** This uses chained CLI commands to obtain the access token from the Account Manager Service.
    **
    ** The first step is to obtain the IP address and Port for the Account Manager Service from the
    **   Chunk Manager Service (really should be renamed). The Chunk Manager Service provides two primary
    **   services, the ownership of service IPs and Port and the management of chunks of data on the
    **   various Storage Servers.
     */
    private final AtomicInteger testCount;

    GetAccessTokenSimple(final InetAddress serverLookupIpAddr, final int serverLookupTcpPort, final String serviceName,
                         final String customer, final String tenancy, final String user, final String password,
                         final AtomicInteger testCount) {

        this.tenancy = tenancy;
        this.customer = customer;
        this.user = user;
        this.password = password;

        this.testCount = testCount;

        cliMemoryManager = new MemoryManager(flavor);

        ServerIdentifierTableMgr serverTableMgr = new LocalServersMgr(flavor);

        clientContextPool = new ClientContextPool(flavor, cliMemoryManager, serverTableMgr);
        cliClient = new NioCliClient(clientContextPool);
        cliClient.start();

        this.eventThreadId = cliClient.getEventThread().getEventPollThreadBaseId();

        contentParser = new ServiceListContentParser();


        ListServersParams params = new ListServersParams(StorageTierEnum.STANDARD_TIER, serviceName, 0, true);
        params.setOpcClientRequestId("GetAccessTokenSimple-7-31-2020.01");

        cli = new ClientCommandInterface(cliClient, cliMemoryManager, serverLookupIpAddr, serverLookupTcpPort,
                params, contentParser, testCount);
    }

    public void execute() {
        cli.executeWithoutMemPoolRelease();

        List<ServerIdentifier> servers = new LinkedList<>();
        contentParser.extractServers(servers);

        if (servers.size() == 1) {
            /*
             ** At his point, the server address being looked for is present (or should be). Now send the request to the
             **   actual service provider.
             */
            GetAccessTokenParams tokenParams = new GetAccessTokenParams(tenancy, customer, user, password);
            tokenParams.setOpcClientRequestId("GetAccessTokenSimple-7-31-2020.02");

            ServerIdentifier server = servers.get(0);
            ClientCommandInterface tokenRequestCli = new ClientCommandInterface(cliClient, cliMemoryManager, server.getServerIpAddress(),
                    server.getServerTcpPort(), tokenParams, testCount);

            tokenRequestCli.execute();

            /*
            ** Want to decrement the count after the chained request completes to insure anything waiting for the test
            **   count to go to 0 doesn't hit a window between the first command finishing and the second one starting.
             */
            cli.decrementCount();
        } else {
            /*
            ** Need to make sure and clean up the memory pool and decrement the number of running requests if there
            **   was an error in the request for the service information
             */
            cli.cleanup();
        }

        cliClient.stop();
        clientContextPool.stop(eventThreadId);
    }

}
