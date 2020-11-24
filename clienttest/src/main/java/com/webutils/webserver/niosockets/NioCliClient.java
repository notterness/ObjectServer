package com.webutils.webserver.niosockets;

import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.ClientRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class NioCliClient {

    private static final Logger LOG = LoggerFactory.getLogger(NioCliClient.class);

    private static final int CLIENT_TEST_BASE_ID = 3000;
    private static final int EVENT_POLL_BALANCER_OFFSET = 10;

    private final ClientContextPool requestContextPool;

    private long nextTransactionId;

    private NioEventPollBalancer eventPollBalancer;

    private final AtomicBoolean threadExit;

    private String failedTestName;

    public NioCliClient(final ClientContextPool contextPool) {
        this.requestContextPool = contextPool;

        nextTransactionId = 1;
        threadExit = new AtomicBoolean(false);

        failedTestName = null;
    }

    public void start() {

        final int NUM_POLL_THREADS = 1;

        LOG.info("start() NUM_POLL_THREADS: " + NUM_POLL_THREADS + " threadBaseId: " + CLIENT_TEST_BASE_ID +
                " offset: " + EVENT_POLL_BALANCER_OFFSET);

        /*
         ** First start the client NIO event poll threads
         */
        eventPollBalancer = new NioEventPollBalancer(NUM_POLL_THREADS, CLIENT_TEST_BASE_ID + EVENT_POLL_BALANCER_OFFSET,
                requestContextPool);
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

    /*
     **
     */
    public ClientRequestContext allocateContext(final int threadId) {
        return requestContextPool.allocateContext(threadId);
    }

    public void releaseContext(final ClientRequestContext requestContext) {
        requestContextPool.releaseContext(requestContext);
    }
}
