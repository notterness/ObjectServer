package com.oracle.athena.webserver.niosockets;


import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;

/*
** This is used to determine which of the event poll threads should be used for this client socket
 */
public class NioEventPollBalancer {

    private static final Logger LOG = LoggerFactory.getLogger(NioEventPollBalancer.class);

    private final int numberPollThreads;
    private final int eventPollThreadBaseId;

    private final WebServerFlavor webServerFlavor;
    private MemoryManager memoryManager;

    private NioEventPollThread[] eventPollThreadPool;

    private int currNioEventThread;

    public NioEventPollBalancer(final WebServerFlavor flavor, final int numPollThreads, final int threadBaseId) {

        this.webServerFlavor = flavor;
        this.numberPollThreads = numPollThreads;
        this.eventPollThreadBaseId = threadBaseId;

        this.memoryManager = new MemoryManager(webServerFlavor);

        eventPollThreadPool = new NioEventPollThread[this.numberPollThreads];
    }

    /*
     ** Start any threads used to handle the event poll loop.
     */
    public void start() {

        currNioEventThread = 0;

        for (int i = 0; i < numberPollThreads; i++) {
            NioEventPollThread pollThread = new NioEventPollThread(webServerFlavor, eventPollThreadBaseId + i, memoryManager);

            pollThread.start();
            eventPollThreadPool[i] = pollThread;
        }

    }

    /*
     ** Stop any threads associated with the event poll loop and cleanup any allocated
     **   resources.
     */
    public void stop() {

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

        LOG.error("NioEventPollBalancer[" + eventThread.getEventPollThreadBaseId() + "] handleAccept()");

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
}
