package com.webutils.webserver.niosockets;


import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import com.webutils.webserver.threadpools.ComputeThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;

/*
** This is used to determine which of the event poll threads should be used for this client socket
 */
public class NioEventPollBalancer {

    private static final Logger LOG = LoggerFactory.getLogger(NioEventPollBalancer.class);

    private static final int COMPUTE_THREAD_ID_OFFSET = 100;

    private final int numberPollThreads;
    private final int eventPollThreadBaseId;

    private final WebServerFlavor webServerFlavor;
    private MemoryManager memoryManager;

    private NioEventPollThread[] eventPollThreadPool;

    private final DbSetup dbSetup;

    /*
    ** The following is a set of threads setup to perform compute work
     */
    private ComputeThreadPool computeThreads;

    private int currNioEventThread;

    public NioEventPollBalancer(final WebServerFlavor flavor, final int numPollThreads, final int threadBaseId,
                                final DbSetup dbSetup) {

        this.webServerFlavor = flavor;
        this.numberPollThreads = numPollThreads;
        this.eventPollThreadBaseId = threadBaseId;
        this.dbSetup = dbSetup;

        this.memoryManager = new MemoryManager(webServerFlavor);

        eventPollThreadPool = new NioEventPollThread[this.numberPollThreads];
    }

    /*
     ** Start any threads used to handle the event poll loop.
     */
    public void start() {

        currNioEventThread = 0;

        for (int i = 0; i < numberPollThreads; i++) {
            NioEventPollThread pollThread = new NioEventPollThread(webServerFlavor, this,
                    eventPollThreadBaseId + i, memoryManager, dbSetup);

            pollThread.start();
            eventPollThreadPool[i] = pollThread;
        }

        computeThreads = new ComputeThreadPool(1, eventPollThreadBaseId + COMPUTE_THREAD_ID_OFFSET);
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

        String memoryPoolOwner = "NioEventPollBalancer[" + eventPollThreadBaseId + "]";
        memoryManager.verifyMemoryPools(memoryPoolOwner);
        memoryManager = null;
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
