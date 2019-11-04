package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ServerChannelLayer implements Runnable {

    public static final int baseTcpPort = 5000;

    private int portNum;
    private int workerThreads;
    private int serverClientId;

    private boolean exitThreads;
    private final int CHAN_TIMEOUT = 100;
    private final int WORK_QUEUE_SIZE = 10;

    private Thread serverAcceptThread;

    private ServerLoadBalancer serverWorkHandler;
    private MemoryManager memoryManager;

    private AsynchronousServerSocketChannel serverChannel;
    private AsynchronousChannelGroup serverCbThreadpool;

    private int serverConnTransactionId;

    private ByteBufferHttpParser byteBufferHttpParser;


    public ServerChannelLayer(int numWorkerThreads, int listenPort, int clientId) {
        portNum = listenPort;
        workerThreads = numWorkerThreads;
        serverClientId = clientId;

        memoryManager = new MemoryManager();

        serverConnTransactionId = 0x5555;

        exitThreads = false;
    }

    public void start() {
        serverWorkHandler = new ServerLoadBalancer(WORK_QUEUE_SIZE, workerThreads, memoryManager,
                (serverClientId * 100));
        serverWorkHandler.start();

        serverAcceptThread = new Thread(this);
        serverAcceptThread.start();
    }

    /*
     ** Perform an orderly shutdown of the server channel and all of its associated resources.
     */
    public void stop() {

        serverWorkHandler.stop();

        /*
         */
        try {
            serverChannel.close();
        } catch (IOException io_ex) {
            System.out.println("Unable to close server socket: " + serverConnTransactionId + " " + io_ex.getMessage());
        }

        /*
         ** Shutdown the AsynchronousChannelGroup and wait for it to cleanup
         */
        serverCbThreadpool.shutdown();

        try {
            boolean shutdown = serverCbThreadpool.awaitTermination(CHAN_TIMEOUT, TimeUnit.MILLISECONDS);
            if (!shutdown) {
                System.out.println("Wait for threadpool shutdown timed out: " + serverConnTransactionId);
            }
        } catch (InterruptedException int_ex) {
            System.out.println("Wait for threadpool shutdown failed: " + serverConnTransactionId + " " + int_ex.getMessage());
        }
    }

    public ServerLoadBalancer getLoadBalancer() {
        System.out.println("ServerChannelLayer(" + serverClientId + ") getLoadBalancer() " + Thread.currentThread().getName());

        return serverWorkHandler;
    }

    public void run() {

        System.out.println("ServerChannelLayer(" + serverClientId + ") start " + Thread.currentThread().getName());

        try {
            serverCbThreadpool = AsynchronousChannelGroup.withFixedThreadPool(WORK_QUEUE_SIZE, Executors.defaultThreadFactory());
        } catch (IOException io_ex) {
            System.out.println("Unable to create server threadpool " + io_ex.getMessage());
            return;
        }

        try {
            serverChannel = AsynchronousServerSocketChannel.open(serverCbThreadpool);

            InetSocketAddress serverAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), portNum);

            System.out.println("Server run(" + serverClientId + "): server: " + serverAddr);

            serverChannel.bind(serverAddr);

        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // running infinite loop for getting
        // client request
        while (!exitThreads) {
            // Accept the incoming request

            /*
            ** If the accept() with a callback is used, the thread must wait until the accept callback
            ** has taken place before calling accept again. Otherwise, there will be an error due to
            ** trying multiple accepts.
            **
            serverChannel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
                public void completed(AsynchronousSocketChannel clientChan, Void att) {
                    System.out.println("Server run(): accept completed: ");
                    serverChannel.accept(null, this);
                    selectHandler.handleAccept(clientChan);
                }

                public void failed(Throwable ex, Void att) {
                    System.out.println("Server run(): accept error: " + ex.getMessage());
                }
            });
            */
            Future<AsynchronousSocketChannel> clientAcceptChan = serverChannel.accept();
            System.out.println("Server run(" + serverClientId + "): waiting on accept");

            try {
                AsynchronousSocketChannel clientChan = clientAcceptChan.get();
                if (!serverWorkHandler.startNewConnection(clientChan)) {
                    /*
                     ** TODO: Need to return error to the client as there are no available ConnectionState objects
                     ** to track this connection.
                     */

                }
            } catch (ExecutionException | InterruptedException ex) {
                System.out.println("Server run(): accept error: " + ex.getMessage());
                exitThreads = true;
            }
        }

        System.out.println("ServerChannelLayer(" + serverClientId + ") exit " + Thread.currentThread().getName());
    }
}
