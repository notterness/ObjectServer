package com.webutils.webserver.niosockets;

import com.webutils.webserver.requestcontext.RequestContextPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class NioServerHandler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(NioServerHandler.class);

    private final int SELECT_TIMEOUT = 1000;

    public static final int OBJECT_SERVER_BASE_ID = 1000;
    public static final int STORAGE_SERVER_BASE_ID = 2000;
    public static final int CHUNK_ALLOC_BASE_ID = 5000;
    public static final int ACCOUNT_MGR_ALLOC_BASE_ID = 6000;

    private static final int NUM_POLL_THREADS = 2;

    private final int tcpListenPort;
    private final int serverThreadBaseId;

    private final RequestContextPool requestContextPool;

    private volatile boolean threadRunning;

    private Selector serverSelector;
    private ServerSocketChannel serverSocketChannel;

    private NioEventPollBalancer eventPollBalancer;
    private Thread serverAcceptThread;

    public NioServerHandler(final int tcpListenPort, final int threadBaseId, final RequestContextPool requestContextPool) {

        this.tcpListenPort = tcpListenPort;
        this.serverThreadBaseId = threadBaseId;

        this.requestContextPool = requestContextPool;

        this.threadRunning = true;
    }

    public void start() {
        /*
        ** First start the client NIO event poll threads
         */
        LOG.info("NioServerHandler[" + serverThreadBaseId + "] start() threads: " + NUM_POLL_THREADS);
        eventPollBalancer = new NioEventPollBalancer(NUM_POLL_THREADS, serverThreadBaseId + 10, requestContextPool);
        eventPollBalancer.start();

        /*
        ** Next start the thread to handle accept() operations from the ServerSocketChannel
         */
        serverAcceptThread = new Thread(this);
        serverAcceptThread.start();
    }

    public void stop() {

        LOG.info("NioServerHandler[" + serverThreadBaseId + "] stop()");

        threadRunning = false;

        try {
            serverAcceptThread.join(1000);
        } catch (InterruptedException int_ex) {
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] join() failed: " + int_ex.getMessage());
        }

        eventPollBalancer.stop();
    }

    /*
    ** This is called when a new server connection is made and the handling RequestContext needs to be
    **   associated with the connection.
     */
    public void setRegistrationCallback() {

    }

    public void run() {

        LOG.info("NioServerHandler[" + serverThreadBaseId + "] run()");

        serverSelector = setupSelector();
        if (serverSelector == null) {
            return;
        }

        serverSocketChannel = setupServerChannel();
        if (serverSocketChannel == null) {
            closeSelector();
            return;
        }

        if (!setOpAccept()) {
            closeServerChannel(serverSocketChannel);
            closeSelector();
            return;
        }

        while (threadRunning) {
            /*
            ** Wait for an OP_ACCEPT event
             */
            int readyChannels;

            try {
                readyChannels = serverSelector.select(SELECT_TIMEOUT);
            } catch (IOException io_ex) {
                LOG.error("NioServerHandler[" + serverThreadBaseId + "] select() failed: " + io_ex.getMessage());
                break;
            }

            if (readyChannels == 0) {
                continue;
            }

            Iterator<SelectionKey> keyIter = serverSelector.selectedKeys().iterator();
            while (keyIter.hasNext()) {
                SelectionKey key = keyIter.next();

                if (key.isAcceptable()) {
                    handleAccept(key);
                }

                keyIter.remove();
            }
        }

        /*
        ** Close out the resources started to accept connections
         */
        closeServerChannel(serverSocketChannel);
        closeSelector();

        LOG.info("NioServerHandler[" + serverThreadBaseId + "] thread finished");

    }

    /*
    ** Handle the server socket channel accept() and spawn off a new client channel to perform
    **   work on.
     */
    private boolean handleAccept(SelectionKey key) {
        boolean success = false;
        SocketChannel clientChannel;

        try {
            clientChannel = ((ServerSocketChannel) key.channel()).accept();
        } catch(IOException io_ex) {
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] handleAccept(1) failed: " + io_ex.getMessage());
            clientChannel = null;
        }

        if (clientChannel != null) {
            try {
                clientChannel.configureBlocking(false);

                /*
                ** Now call the "registration" callback (currently a fixed call until the setRegistrationCallback()
                **   method is wired through).
                 */
                if (!eventPollBalancer.registerClientSocket(clientChannel)) {
                    try {
                        clientChannel.close();
                    } catch (IOException io_ex) {
                        LOG.error("NioServerHandler[" + serverThreadBaseId + "] clientChannel.close() failed: " + io_ex.getMessage());
                    }
                }
            } catch (IOException io_ex) {
                LOG.error("NioServerHandler[" + serverThreadBaseId + "] handleAccept(2) failed: " + io_ex.getMessage());
            }
        }

        return success;
    }

    /*
    ** closeSelector() is simply a way to hide the exception handling for Selector.close(). Since this is
    **   only called when shutting down, we don't really care if it failed or not. But, for code correctness,
    **   it is good to know why it failed if it is something that can be fixed.
     */
    private void closeSelector() {
        try {
            serverSelector.close();
        } catch (IOException io_ex) {
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] Selector.close() failed: " + io_ex.getMessage());
        }
    }

    /*
    ** closeServerChannel() is simply a way to hide the exception handling for ServerSocketChannel.close()
     */
    private void closeServerChannel(ServerSocketChannel serverSocket) {
        try {
            serverSocket.close();
        } catch (IOException io_ex) {
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] close() failed: " + io_ex.getMessage());
        }
    }

    /*
    ** Set the OP_ACCEPT key for the server socket channel.
    **   This is done within its own method to help make the handling of all of the possible
    **   exceptions easier.
     */
    private boolean setOpAccept() {

        try {
            serverSocketChannel.register(serverSelector, SelectionKey.OP_ACCEPT);
        } catch (IOException io_ex) {
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] register() failed: " + io_ex.getMessage());
            return false;
        }
        return true;
    }

    /*
    **
     */
    private Selector setupSelector() {
        Selector serverSelector;

        try {
            serverSelector = Selector.open();
        } catch (IOException io_ex) {
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] Selector.open() failed: " + io_ex.getMessage());
            return null;
        }

        return serverSelector;
    }


    /*
    ** Setup the server socket channel to be ready to accept connections.
     */
    private ServerSocketChannel setupServerChannel() {
        ServerSocketChannel serverChannel;
        try {
            serverChannel = ServerSocketChannel.open();
        } catch (IOException io_ex) {
            /*
             ** This is a fatal error if the WebServer cannot accept connections
             */
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] open() failed: " + io_ex.getMessage());
            return null;
        }

        InetSocketAddress serverAddr;

        /*
        ** DEBUG code only
        * */
        try {
            LOG.info("getLocalHost: " + InetAddress.getLocalHost().toString() + " getLoopbackAddress: " + InetAddress.getLoopbackAddress().toString());
        } catch (UnknownHostException ex) {
            System.out.println("LocalHost is unknown");
        }

        try {
            serverAddr = new InetSocketAddress(InetAddress.getLocalHost(), tcpListenPort);
        } catch (UnknownHostException ex) {
            System.out.println("LocalHost is unknown");
            return null;
        }
        LOG.info("NioServerHandler[" + serverThreadBaseId + "] run() serverAddr: " + serverAddr);
        System.out.println("NioServerHandler[" + serverThreadBaseId + "] run() serverAddr: " + serverAddr);

        try {
            serverChannel.socket().bind(serverAddr);
        } catch (IOException io_ex) {
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] bind() failed: " + io_ex.getMessage());
            closeServerChannel(serverChannel);
            return null;
        }

        /*
        **
         */
        try {
            serverChannel.configureBlocking(false);
        } catch (IOException ex) {
            LOG.error("NioServerHandler[" + serverThreadBaseId + "] configureBlocking() failed: " + ex.getMessage());
            closeServerChannel(serverChannel);
            return null;
        }

        return serverChannel;
    }
}
