package com.oracle.athena.webserver.niosockets;

import com.oracle.athena.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.LinkedList;

public class NioEventPollThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(NioEventPollThread.class);

    private static final int ALLOCATED_NIO_SOCKET = 10;

    private final int SELECT_TIMEOUT = 1000;

    private final int eventPollThreadBaseId;

    private final LinkedList<NioSocket> freeConnections;
    private final LinkedList<Operation> waitingForConnections;

    private volatile boolean threadRunning;

    private Thread eventPollThread;

    private NioSelectHandler nioSelectHandler;


    /*
    ** This is the Selector used to handle READ and WRITE events for the client SocketChannel.
     */
    private Selector clientSocketSelector;


    public NioEventPollThread(final int threadBaseId) {
        this.eventPollThreadBaseId = threadBaseId;

        this.freeConnections = new LinkedList<NioSocket>();
        this.waitingForConnections = new LinkedList<Operation>();

        this.threadRunning = true;
    }

    /*
    ** Setup the Thread to handle the event loops for the SocketChannel
     */
    void start() {
        /*
        ** Setup NioSelectHandler for this thread.
         */
        nioSelectHandler = new NioSelectHandler();

        /*
        ** Now setup the Selector for the handler so it can be passed into the pre-allocated NioSocket
        **   connection control objects.
         */
        Selector selector = nioSelectHandler.setupSelector();

        /*
        ** Create a collection of NioSocket to handle communications with the Storage Servers
         */
        for (int i = 0; i < ALLOCATED_NIO_SOCKET; i++) {
            NioSocket connection = new NioSocket(nioSelectHandler);

            freeConnections.add(connection);
        }

        eventPollThread = new Thread(this);
        eventPollThread.start();
    }

    /*
    ** Shutdown the Thread used to handle the event loop
     */
    void stop() {
        threadRunning = false;
    }

    /*
    ** TODO: Wire in the wakeup of the waitingOperation if there are no NioSocket
    **   available and add a test for this
     */
    public NioSocket allocateConnection(final Operation waitingOperation) {
        NioSocket connection =  freeConnections.poll();
        if (connection == null) {
            waitingForConnections.add(waitingOperation);
        }

        return connection;
    }

    public void releaseConnection(final NioSocket connection) {
        freeConnections.add(connection);

        Operation waitingOperation = waitingForConnections.poll();
        if (waitingOperation != null) {
            waitingOperation.event();
        }
    }

    /*
    ** This is where a RequestContext is acquired for a connection and the association between the connection and
    **   the SocketChannel is made. This is how the NIO layer is linked into the actual RequestContext and its
    **   associated BufferManagers.
    ** Add a client SocketChannel to the Selector
     */
    boolean registerClientSocket(final SocketChannel clientChannel) {
        boolean success = true;
        try {
            clientChannel.register(clientSocketSelector, SelectionKey.OP_READ);
        } catch (ClosedChannelException | ClosedSelectorException | IllegalSelectorException ex) {
            LOG.error("registerClientSocket[" + eventPollThreadBaseId + "] failed: " + ex.getMessage());

            success = false;
        }

        return success;
    }


    /*
    ** The following is the Thread to handle the select events for the client SocketChannel
     */
    public void run() {

        while (threadRunning) {
            /*
            ** Perform the NIO SocketChannel work
             */
            nioSelectHandler.handleSelector();

            /*
            ** Now check if there is other work to be performed on the connections that does not deal with the
            **   SocketChanel read and write operations
             */
        }
    }

    /*
    **
     */
    void handleRead(final NioSocket connection) {

    }

    /*
    **
     */
    void handleWrite(final NioSocket connection) {

    }

}
