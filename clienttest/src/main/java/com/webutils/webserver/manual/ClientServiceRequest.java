package com.webutils.webserver.manual;

import com.webutils.webserver.common.ObjectParams;
import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.operations.ClientCallbackOperation;
import com.webutils.webserver.operations.SendRequestToService;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientServiceRequest extends ClientInterface {

    private final ObjectParams requestParams;

    private final ContentParser contentParser;

    private final NioCliClient client;
    private final int eventThreadId;

    private static final String clientName = "ObjectCLI/0.0.1";

    ClientServiceRequest(final NioCliClient cliClient, final MemoryManager memoryManger, final InetAddress serverIpAddr, final int serverTcpPort,
                         final ObjectParams serviceRequestParams, final ContentParser contentParser, AtomicInteger testCount) {

        super(cliClient, memoryManger, serverIpAddr, serverTcpPort, testCount);

        this.requestParams = serviceRequestParams;
        this.contentParser = contentParser;

        /*
         ** The testClient is responsible for providing the threads the Operation(s) will run on and the
         **   NIO Socket handling.
         */
        this.client = cliClient;
        EventPollThread eventThread = cliClient.getEventThread();
        this.eventThreadId = eventThread.getEventPollThreadBaseId();
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
         ** Create the ClientGetObject operation and connect in this object to provide the HTTP header
         **   generator
         */
        ServerIdentifier service = new ServerIdentifier("ObjectCLI", serverIpAddr, serverTcpPort, 0);
        HttpResponseInfo httpInfo = new HttpResponseInfo(clientContext.getRequestId());
        service.setHttpInfo(httpInfo);

        ClientCallbackOperation callbackOp = new ClientCallbackOperation(clientContext, this, httpInfo);

        SendRequestToService cmdSend = new SendRequestToService(clientContext, memoryManager, service, requestParams,
                contentParser, callbackOp);
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
            System.out.println("waitForRequestComplete() timed out");
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
