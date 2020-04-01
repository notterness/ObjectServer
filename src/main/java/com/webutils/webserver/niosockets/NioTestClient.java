package com.webutils.webserver.niosockets;

import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.NioEventPollBalancer;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.util.concurrent.atomic.AtomicBoolean;


/*
** A NioTestClient is a single connection that can talk to one server to perform an entire
**   HTTP Request including receiving the final status. It is used to test various behaviors
**   of the WebServer to insure they are correct.
*/
public class NioTestClient {

    private static final int WORK_QUEUE_SIZE = 10;

    private final int clientThreadBaseId;

    private long nextTransactionId;

    private NioEventPollBalancer eventPollBalancer;

    private final AtomicBoolean threadExit;

    private String failedTestName;

    public NioTestClient(final int clientThreadBaseId) {
        this.clientThreadBaseId = clientThreadBaseId;

        nextTransactionId = 1;
        threadExit = new AtomicBoolean(false);

        failedTestName = null;
    }

    public void start() {

        /*
         ** First start the client NIO event poll threads
         */
        eventPollBalancer = new NioEventPollBalancer(1, clientThreadBaseId + 10, null);
        eventPollBalancer.start();
    }

    public void stop() {
        /*
        ** Shutdown the EventPollBalancer which will in turn shut down the EventThread(s) and release the resources.
         */
        eventPollBalancer.stop();

        threadExit.set(true);
    }

    public EventPollThread getEventThread() {
        return eventPollBalancer.getNextEventThread();
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
