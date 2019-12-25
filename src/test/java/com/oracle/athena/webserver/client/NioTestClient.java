package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.niosockets.EventPollThread;
import com.oracle.athena.webserver.niosockets.NioEventPollBalancer;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;

import java.util.concurrent.atomic.AtomicBoolean;


/*
** A NioTestClient is a single connection that can talk to one server to perform an entire
**   HTTP Request including receiving the final status. It is used to test various behaviors
**   of the WebServer to insure they are corect.
*/
public class NioTestClient implements Runnable {

    private static final int WORK_QUEUE_SIZE = 10;

    private final int serverTcpPort;
    private final String testName;

    private final int clientThreadBaseId;

    private long nextTransactionId;

    /*
    ** For a single HTTP Request, there only needs to be a single eventThread to send out the request and
    **   receive the response.
     */
    private EventPollThread eventThread;
    private RequestContext clientRequest;

    private NioEventPollBalancer eventPollBalancer;

    private Thread clientWriteThread;

    private final AtomicBoolean threadExit;

    private String failedTestName;

    public NioTestClient(final String testName, final int targetTcpPort, final int clientThreadBaseId) {
        this.testName = testName;
        this.serverTcpPort = targetTcpPort;
        this.clientThreadBaseId = clientThreadBaseId;

        nextTransactionId = 1;
        threadExit = new AtomicBoolean(false);

        failedTestName = null;
    }

    public void start() {

        /*
         ** First start the client NIO event poll threads
         */
        eventPollBalancer = new NioEventPollBalancer(WebServerFlavor.INTEGRATION_TESTS, 1, clientThreadBaseId + 10);
        eventPollBalancer.start();

        eventThread = eventPollBalancer.getNextEventThread();

        clientWriteThread = new Thread(this);
        clientWriteThread.start();

    }

    public void stop() {
        /*
        ** Return the RequestContext back to the free pool
         */
        eventThread.releaseContext(clientRequest);

        /*
        ** Shutdown the EventPollBalancer which will in turn shut down the EventThread(s) and release the resources.
         */
        eventPollBalancer.stop();

        threadExit.set(true);
    }

    public EventPollThread getEventThread() {
        return eventThread;
    }

    /*
     **
     ** TODO: The single thread to handle all the writes doesn't really work since the writes
     **   per connection need to be ordered and a write needs to complete before the next
     **   one is allowed to start. Something more like a map that states a connection has
     **   work pending and then calling the writeAvailableData() might be a better solution.
     */
    public void run() {
        System.out.println("TestClient thread() start");

        System.out.println("TestClient thread() exit");
    }


    private synchronized long getTransactionId() {
        long transaction = nextTransactionId;

        nextTransactionId++;

        return transaction;
    }

    /*
    ** This logs the first test that failed. If multiple tests fail, only the first one is retained
     */
    public void setTestFailed(final String failedTest) {
        if (failedTestName == null) {
            System.out.println("Adding test failure: " + failedTest);

            failedTestName = failedTest;
        } else {
            System.out.println("Cannot add test failure: " + failedTest);
        }
    }

    public String getFailedTestName() {
        return failedTestName;
    }

}
