package com.webutils.webserver.manual;

import com.webutils.webserver.common.ObjectParams;
import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.operations.ClientCommandSend;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientCommandInterface extends ClientInterface {

    private final ObjectParams requestParams;

    private final ContentParser contentParser;

    private final NioCliClient client;
    private final EventPollThread eventThread;
    private final int eventThreadId;

    private static final String clientName = "ObjectCLI/0.0.1";

    /*
    ** There are two forms of the ClientCommandInterface. The first just uses the parser provided by ConvertRespBodyToString
    **   to build a String that can be output on the console. The second uses a passed in ContentParser to pull the
    **   information returned in the content response into a form that can be used by the caller.
     */
    ClientCommandInterface(final NioCliClient cliClient, final MemoryManager memoryManger, final InetAddress serverIpAddr, final int serverTcpPort,
                           final ObjectParams requestParams, AtomicInteger testCount) {
        this(cliClient, memoryManger, serverIpAddr, serverTcpPort, requestParams, null, testCount);
    }

    ClientCommandInterface(final NioCliClient cliClient, final MemoryManager memoryManger, final InetAddress serverIpAddr, final int serverTcpPort,
                           final ObjectParams requestParams, final ContentParser contentParser, AtomicInteger testCount) {
        super(cliClient, memoryManger, serverIpAddr, serverTcpPort, testCount);

        this.requestParams = requestParams;
        this.contentParser = contentParser;

        /*
         ** The testClient is responsible for providing the threads the Operation(s) will run on and the
         **   NIO Socket handling.
         */
        this.client = cliClient;
        this.eventThread = cliClient.getEventThread();
        this.eventThreadId = this.eventThread.getEventPollThreadBaseId();
    }

    /*
    ** This is used to execute a CLI request that is part of a chained operation. It will not release
    **   the memory and will not decrement the number of running commands.
     */
    public void executeWithoutMemPoolRelease() {
        /*
         ** Allocate a RequestContext
         */
        ClientRequestContext clientContext = client.allocateContext(eventThreadId);

        /*
         ** Allocate an IoInterface to use and assign it to the ClientRequestContext
         */
        IoInterface connection = eventThread.allocateConnection(null);
        clientContext.initializeServer(connection, 1);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException int_ex) {
            int_ex.printStackTrace();
        }

        /*
         ** Create the ClientGetObject operation and connect in this object to provide the HTTP header
         **   generator
         */
        ServerIdentifier objectServer = new ServerIdentifier("ObjectCLI", serverIpAddr, serverTcpPort, 0);

        HttpResponseInfo httpResponseInfo = new HttpResponseInfo(clientContext.getRequestId());
        objectServer.setHttpInfo(httpResponseInfo);
        System.out.println("\nSTARTING ClientCommandInterface - " + requestParams.getOpcClientRequestId());

        ClientCommandSend cmdSend = new ClientCommandSend(this, clientContext, memoryManager, objectServer,
                requestParams, contentParser);
        cmdSend.initialize();

        /*
         ** Start the process of sending the HTTP Request and the request object
         */
        cmdSend.event();

        /*
         ** Now wait for the status to be received and then it can verified with the expected value
         */
        if (!waitForRequestComplete()) {
            /*
             ** Delete the file and send out an error
             */
        }

        /*
         ** Cleanup out the ClientCommandSend Operation
         */
        cmdSend.complete();

        client.releaseContext(clientContext);
    }

    /*
    ** This is the standard execute() function if there are not multiple operations that are chained and using
    **   the same memory pool
     */
    public void execute() {
        executeWithoutMemPoolRelease();

        /*
        ** Now release the memory in the pool and then decrement the running test count.
         */
        memoryManager.verifyMemoryPools(clientName);

        runningTestCount.decrementAndGet();
    }

    /*
    ** This is used if the CLI command the used the executeWithoutMemPoolRelease() had and
    **   error and the request that was to follow it will not be run.
     */
    public void cleanup() {
        /*
         ** Now release the memory in the pool and then decrement the running test count.
         */
        memoryManager.verifyMemoryPools(clientName);

        runningTestCount.decrementAndGet();
    }

    public void decrementCount() {
        runningTestCount.decrementAndGet();
    }
}
