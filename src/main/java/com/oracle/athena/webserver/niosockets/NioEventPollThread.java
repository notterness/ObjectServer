package com.oracle.athena.webserver.niosockets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Iterator;

public class NioEventPollThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(NioEventPollThread.class);

    private final int SELECT_TIMEOUT = 1000;

    private final int eventPollThreadBaseId;

    private volatile boolean threadRunning;

    private Thread eventPollThread;

    /*
    ** This is the Selector used to handle READ and WRITE events for the client SocketChannel.
     */
    private Selector clientSocketSelector;


    NioEventPollThread(final int threadBaseId) {
        this.eventPollThreadBaseId = threadBaseId;

        this.threadRunning = true;
    }

    /*
    ** Setup the Thread to handle the event loops for the SocketChannel
     */
    void start() {
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
    ** This is where a RequestContext is aquired for a connection and the association between the connection and
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
        int readyChannels;

        Selector clientSocketSelector = setupSelector();
        if (clientSocketSelector == null) {
            return;
        }

        while (threadRunning) {
            try {
                readyChannels = clientSocketSelector.select(SELECT_TIMEOUT);
            } catch (IOException io_ex) {
                LOG.error("NioEventPollthread[" + eventPollThreadBaseId + "] select() failed: " + io_ex.getMessage());

                /*
                ** Force exit due to a serious error
                 */
                threadRunning = false;
                break;
            }

            if (readyChannels > 0) {
                Iterator<SelectionKey> keyIter = clientSocketSelector.selectedKeys().iterator();
                while (keyIter.hasNext()) {
                    SelectionKey key = keyIter.next();

                    if (key.isReadable()) {
                        handleRead(key);
                    }

                    if (key.isWritable()) {
                        handleWrite(key);
                    }

                    keyIter.remove();
                }

            }

            /*
            ** Now check if there is other work to be performed on the connections that does not deal with the
            **   SocketChanel read and write operations
             */
        }
    }

    /*
    **
     */
    void handleRead(SelectionKey key) {

    }

    /*
    **
     */
    void handleWrite(SelectionKey key) {

    }


    /*
     ** This sets up a Selector for use with the client SocketChannel(s) that are being operated on.
     **   A SocketChannel has a one to one relationship with a connection that is used to handle an
     **   HTTP request.
     */
    private Selector setupSelector() {
        Selector clientSelector;

        try {
            clientSelector = Selector.open();
        } catch (IOException io_ex) {
            LOG.error("NioEventPollthread[" + eventPollThreadBaseId + "] Selector.open() failed: " + io_ex.getMessage());
            return null;
        }

        return clientSelector;
    }


}
