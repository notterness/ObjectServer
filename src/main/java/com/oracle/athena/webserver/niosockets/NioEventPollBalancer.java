package com.oracle.athena.webserver.niosockets;


import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;

import java.nio.channels.SocketChannel;

/*
** This is used to determine which of the event poll threads should be used for this client socket
 */
public class NioEventPollBalancer {

    private final int numberPollThreads;
    private final int eventPollThreadBaseId;

    private final WebServerFlavor webServerFlavor;
    private MemoryManager memoryManager;

    private NioEventPollThread[] eventPollThreadPool;

    private int currNioEventThread;

    NioEventPollBalancer(final WebServerFlavor flavor, final int numPollThreads, final int threadBaseId) {

        this.webServerFlavor = flavor;
        this.numberPollThreads = numPollThreads;
        this.eventPollThreadBaseId = threadBaseId;

        this.memoryManager = new MemoryManager(webServerFlavor);

        eventPollThreadPool = new NioEventPollThread[this.numberPollThreads];
    }

    /*
    ** Start any threads used to handle the event poll loop.
     */
    void start() {

        currNioEventThread = 0;

        for (int i = 0; i < numberPollThreads; i++) {
            NioEventPollThread pollThread = new NioEventPollThread(eventPollThreadBaseId + i, memoryManager);

            pollThread.start();
            eventPollThreadPool[i] = pollThread;
        }

    }

    /*
    ** Stop any threads associated with the event poll loop and cleanup any allocated
    **   resources.
     */
    void stop() {

        for (int i = 0; i < numberPollThreads; i++) {
            NioEventPollThread pollThread = eventPollThreadPool[i];
            eventPollThreadPool[i] = null;
            pollThread.stop();
        }

        memoryManager = null;
    }

    /*
    ** Add this socket to one the event poll work threads.
    **
    ** TODO: This does not need to be synchronized if there is only a single acceptor thread.
     */
    synchronized boolean registerClientSocket(SocketChannel clientChannel) {
        boolean success = eventPollThreadPool[currNioEventThread].registerClientSocket(clientChannel);

        currNioEventThread++;
        if (currNioEventThread == numberPollThreads) {
            currNioEventThread = 0;
        }

        return success;
    }
}
