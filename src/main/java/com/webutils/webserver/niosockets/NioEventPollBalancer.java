package com.webutils.webserver.niosockets;


import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.requestcontext.RequestContextPool;
import com.webutils.webserver.threadpools.ComputeThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;

/*
** This is used to determine which of the event poll threads should be used for this client socket
 */
public class NioEventPollBalancer {

    private static final Logger LOG = LoggerFactory.getLogger(NioEventPollBalancer.class);

    /*
    ** The maximum number of ComputeThreads is limited to 20. If more than 20 compute threads are allocated then the
    **   threadBaseId + COMPUTE_THREAD_ID_OFFSET + numCompiuteThreads will corss over the boundary that is impossed
    **   by the STORAGE_SERVER_BASE_ID_OFFSET.
    ** Since the Storage Servers are allocated using a base ID and then are separated by the STORAGE_SERVER_BASE_ID_OFFSET,
    **   the number of worker threads plus the number of compute threads cannot exceed the OFFSET.
     */
    private static final int NUM_COMPUTE_THREADS = 1;
    private static final int COMPUTE_THREAD_ID_OFFSET = 80;

    private final int numberPollThreads;
    private final int eventPollThreadBaseId;

    private NioEventPollThread[] eventPollThreadPool;

    private final RequestContextPool requestContextPool;

    /*
    ** The following is a set of threads setup to perform compute work
     */
    private ComputeThreadPool computeThreads;

    private int currNioEventThread;

    public NioEventPollBalancer(final int numPollThreads, final int threadBaseId, final RequestContextPool requestContextPool) {

        LOG.info("NioEventPollBalancer[" + threadBaseId + "] numPollThreads: " + numPollThreads);

        this.numberPollThreads = numPollThreads;
        this.eventPollThreadBaseId = threadBaseId;
        this.requestContextPool = requestContextPool;

        eventPollThreadPool = new NioEventPollThread[this.numberPollThreads];
    }

    /*
     ** Start any threads used to handle the event poll loop.
     */
    public void start() {

        currNioEventThread = 0;

        for (int i = 0; i < numberPollThreads; i++) {
            NioEventPollThread pollThread = new NioEventPollThread(this,eventPollThreadBaseId + i, requestContextPool);

            pollThread.start();
            eventPollThreadPool[i] = pollThread;
        }

        computeThreads = new ComputeThreadPool(NUM_COMPUTE_THREADS, eventPollThreadBaseId + COMPUTE_THREAD_ID_OFFSET);
        computeThreads.start();
    }

    /*
     ** Stop any threads associated with the event poll loop and cleanup any allocated
     **   resources.
     */
    public void stop() {

        computeThreads.stop();
        computeThreads = null;

        for (int i = 0; i < numberPollThreads; i++) {
            NioEventPollThread pollThread = eventPollThreadPool[i];
            eventPollThreadPool[i] = null;
            pollThread.stop();
        }
    }

    /*
     ** Add this socket to one the event poll work threads.
     **
     ** TODO: This does not need to be synchronized if there is only a single acceptor thread.
     */
    synchronized boolean registerClientSocket(SocketChannel clientChannel) {

        EventPollThread eventThread = getNextEventThread();
        boolean success = eventThread.registerClientSocket(clientChannel);

        LOG.info("NioEventPollBalancer[" + eventThread.getEventPollThreadBaseId() + "] handleAccept() success: " + success);

        return success;
    }

    /*
     ** This returns the next "ready" EventPollThread that can be used.
     */
    public synchronized EventPollThread getNextEventThread() {
        NioEventPollThread eventThread = eventPollThreadPool[currNioEventThread];

        currNioEventThread++;
        if (currNioEventThread == numberPollThreads) {
            currNioEventThread = 0;
        }

        return eventThread;
    }

    /*
    ** This adds work to the Compute Thread Pool to be picked up by a free compute thread.
     */
    void runComputeWork(final Operation computeOperation) {
        computeThreads.addComputeWorkToThread(computeOperation);
    }

    void removeComputeWork(final Operation computeOperation) {
        computeThreads.removeFromComputeThread(computeOperation);
    }

}
