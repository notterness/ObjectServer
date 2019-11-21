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

abstract public class ServerChannelLayer implements Runnable {

    public static final int BASE_TCP_PORT = 5000;
    public static final int DEFAULT_CLIENT_ID = 31415;
    private static final int CHAN_TIMEOUT = 100;
    public static final int WORK_QUEUE_SIZE = 10;

    private int portNum;
    //TODO: naming, should indicate it is the number of threads and not the threads themselves.
    int workerThreads;
    //FIXME: this variable and its usage are not production ready.  We should not rely on trace statements in log files
    //for anything in production.
    int serverClientId;

    //FIXME: abstract class should not have a member variable it neither initializes nor uses. Probably delete this.
    public Thread serverAcceptThread;
    //TODO: naming, either class ServerLoadBalancer or variable workHandler needs a new name (without server in it)
    ServerLoadBalancer serverWorkHandler;

    public MemoryManager memoryManager;


    //FIXME: should become a local variable of the run method
    private boolean exitThreads;

    //TODO: naming, should be called channel and cbThreadpool
    private AsynchronousServerSocketChannel serverChannel;
    private AsynchronousChannelGroup serverCbThreadpool;

    //TODO: naming, should not be called server, name does not indicate purpose
    //FIXME: this variable and its usage are not production ready.  We should not rely on trace statements in log files
    //for anything in production.
    private int serverConnTransactionId;

    //FIXME - remove or use instance variable
    private ByteBufferHttpParser byteBufferHttpParser;

    //TODO: constructor parameters should have the same name as the instance variables to which they are assigned, and
    // constructor code should read this.varname = varname
    public ServerChannelLayer(int numWorkerThreads, int listenPort, int clientId) {
        portNum = listenPort;
        workerThreads = numWorkerThreads;
        serverClientId = clientId;

        memoryManager = new MemoryManager();

        serverConnTransactionId = 0x5555;

        exitThreads = false;
    }

    abstract public void start();

    /*
     ** Perform an orderly shutdown of the server channel and all of its associated resources.
     */
    public void stop() {

        //FIXME: does not work with InitiatorServer
        serverWorkHandler.stop();

        /*
         */
        try {
            serverChannel.close();
        } catch (IOException io_ex) {
            //FIXME: handle this
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

    public void run() {

        System.out.println("ServerChannelLayer(" + serverClientId + ") start " + Thread.currentThread().getName());

        try {
            serverCbThreadpool = AsynchronousChannelGroup.withFixedThreadPool(WORK_QUEUE_SIZE, Executors.defaultThreadFactory());
        } catch (IOException io_ex) {
            //FIXME: handle this
            System.out.println("Unable to create server threadpool " + io_ex.getMessage());
            return;
        }

        try {
            serverChannel = AsynchronousServerSocketChannel.open(serverCbThreadpool);

            InetSocketAddress serverAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), portNum);

            System.out.println("Server run(" + serverClientId + "): server: " + serverAddr);

            serverChannel.bind(serverAddr);

        } catch (IOException e) {
            //FIXME: handle this
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
            */
            Future<AsynchronousSocketChannel> clientAcceptChan = serverChannel.accept();
            System.out.println("Server run(" + serverClientId + "): waiting on accept");

            try {
                AsynchronousSocketChannel clientChan = clientAcceptChan.get();

                System.out.println("Server run(" + serverClientId + "): accepted");

                if (!serverWorkHandler.startNewConnection(clientChan)) {
                    /*
                    FIXME: currently this only happens if the load balancer was unable to allocate a connection pool
                    from the unreserved normal pool, and also unable to allocate a connection pool from the unreserved
                    blocking pool. This probably shouldn't be passed up to us via a boolean return type, and depending
                    on the solution we design, might want to be handled at a lower layer.
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
