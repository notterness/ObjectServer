package com.webutils.webserver.manual;

import com.webutils.webserver.common.ObjectParams;
import com.webutils.webserver.http.AllocateChunksResponseContent;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.operations.ClientCallbackOperation;
import com.webutils.webserver.operations.ClientCommandSend;
import com.webutils.webserver.operations.SendRequestToService;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientServiceRequest extends ClientInterface {

    private final ObjectParams requestParams;

    private final NioCliClient client;
    private final EventPollThread eventThread;
    private final int eventThreadId;

    private static final String clientName = "ObjectCLI/0.0.1";

    ClientServiceRequest(final NioCliClient cliClient, final MemoryManager memoryManger, final InetAddress serverIpAddr, final int serverTcpPort,
                         final ObjectParams deleteRequestParams, AtomicInteger testCount) {

        super(cliClient, memoryManger, serverIpAddr, serverTcpPort, testCount);

        this.requestParams = deleteRequestParams;

        /*
         ** The testClient is responsible for providing the threads the Operation(s) will run on and the
         **   NIO Socket handling.
         */
        this.client = cliClient;
        this.eventThread = cliClient.getEventThread();
        this.eventThreadId = this.eventThread.getEventPollThreadBaseId();
    }

    /*
     **
     */
    public void execute() {
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
        ServerIdentifier service = new ServerIdentifier("ObjectCLI", serverIpAddr, serverTcpPort, 0);
        HttpResponseInfo httpInfo = new HttpResponseInfo(clientContext.getRequestId());
        service.setHttpInfo(httpInfo);

        ClientCallbackOperation callbackOp = new ClientCallbackOperation(clientContext, this, httpInfo);

        AllocateChunksResponseContent responseParser = new AllocateChunksResponseContent();
        SendRequestToService cmdSend = new SendRequestToService(clientContext, memoryManager, service, requestParams,
                responseParser, callbackOp);
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

        memoryManager.verifyMemoryPools(clientName);

        runningTestCount.decrementAndGet();
    }

}
