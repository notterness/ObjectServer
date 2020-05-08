package com.webutils.webserver.manual;

import com.webutils.webserver.common.GetObjectParams;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.niosockets.NioCliClient;
import com.webutils.webserver.operations.ClientObjectGet;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientGetInterface extends ClientInterface {

    private final GetObjectParams requestParams;

    private final NioCliClient client;
    private final EventPollThread eventThread;
    private final int eventThreadId;

    private static final String clientName = "ObjectCLI/0.0.1";

    ClientGetInterface(final NioCliClient cliClient, final MemoryManager memoryManger, final InetAddress serverIpAddr, final int serverTcpPort,
                       final GetObjectParams getRequestParams, AtomicInteger testCount) {

        super(cliClient, memoryManger, serverIpAddr, serverTcpPort, testCount);

        this.requestParams = getRequestParams;

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
         ** Create the ClientObjectGet operation and connect in this object to provide the HTTP header
         **   generator
         */
        ServerIdentifier objectServer = new ServerIdentifier("ObjectCLI", serverIpAddr, serverTcpPort, 0);

        HttpResponseInfo httpResponseInfo = new HttpResponseInfo(clientContext);
        objectServer.setHttpInfo(httpResponseInfo);
        ClientObjectGet objectGet = new ClientObjectGet(this, clientContext, memoryManager, objectServer,
                requestParams);
        objectGet.initialize();

        /*
         ** Start the process of sending the HTTP Request and the request object
         */
        objectGet.event();

        /*
         ** Now wait for the status to be received and then it can verified with the expected value
         */
        if (!waitForRequestComplete()) {
            /*
            ** Delete the file and send out an error
             */
        }

        /*
         ** Cleanup out the ClientObjectGet Operation
         */
        objectGet.complete();

        client.releaseContext(clientContext);

        memoryManager.verifyMemoryPools(clientName);

        /*
        ** Write the response status information
         */
        if (httpResponseInfo.getResponseStatus() == HttpStatus.OK_200) {
            System.out.println("Status: OK_200");
        } else {
            System.out.println("Status: " + httpResponseInfo.getHttpParseError());
        }

        runningTestCount.decrementAndGet();
    }

}
