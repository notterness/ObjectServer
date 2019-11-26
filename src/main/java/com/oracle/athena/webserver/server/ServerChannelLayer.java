package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerChannelLayer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerChannelLayer.class);

    public static final int BASE_TCP_PORT = 5000;
    public static final int HTTP_TCP_PORT = BASE_TCP_PORT + 80;
    public static final int HTTPS_TCP_PORT = BASE_TCP_PORT + 443;
    public static final int DEFAULT_CLIENT_ID = 31415;
    private static final int CHAN_TIMEOUT = 100;
    public static final int WORK_QUEUE_SIZE = 10;

    private SSLContext sslContext;

    private int portNum;
    //TODO: naming, should indicate it is the number of threads and not the threads themselves.
    int workerThreads;
    //FIXME: this variable and its usage are not production ready.  We should not rely on trace statements in log files
    //for anything in production.
    int serverClientId;
    boolean ssl;

    //FIXME: abstract class should not have a member variable it neither initializes nor uses. Probably delete this.
    protected Thread serverAcceptThread;
    //TODO: naming, either class ServerLoadBalancer or variable workHandler needs a new name (without server in it)
    ServerLoadBalancer serverWorkHandler;

    protected ServerDigestThreadPool digestThreadPool;

    //FIXME: should become a local variable of the run method
    private boolean exitThreads;

    //TODO: naming, should be called channel and cbThreadpool
    private AsynchronousServerSocketChannel serverChannel;
    private AsynchronousChannelGroup serverCbThreadpool;

    //TODO: naming, should not be called server, name does not indicate purpose
    private int serverConnTransactionId;

    //FIXME - remove or use instance variable
    private ByteBufferHttpParser byteBufferHttpParser;

    public ServerChannelLayer(ServerLoadBalancer serverWorkHandler, int portNum, int serverClientId, boolean ssl) {
        this.serverWorkHandler = serverWorkHandler;
        this.portNum = portNum;
        this.serverClientId = serverClientId;
        this.ssl = ssl;

        if (ssl) {
            try {
                sslContext = SSLContext.getInstance("TLSv1.2");

                // TODO: figure out how to initialize with casper certs
                sslContext.init(createKeyManagers("./src/main/resources/server.jks", "athena", "athena"), createTrustManagers("./src/main/resources/trustedCerts.jks", "athena"), new SecureRandom());

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (KeyManagementException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            sslContext = null;
        }

        serverConnTransactionId = 0x5555;

        serverAcceptThread = new Thread(this);
        exitThreads = false;
    }

    public ServerChannelLayer(ServerLoadBalancer serverWorkHandler, int listenPort, int clientId) {
         this(serverWorkHandler, listenPort, clientId, false);
    }

    public ServerChannelLayer(int listenPort, int clientId) {
        this.serverWorkHandler = null;
        portNum = listenPort;
        serverClientId = clientId;
        ssl = false;
        sslContext = null;

        this.digestThreadPool = new ServerDigestThreadPool(2,10);

        serverConnTransactionId = 0x5555;

        serverAcceptThread = new Thread(this);
        exitThreads = false;
    }

    public void start() {
        serverAcceptThread.start();
    }

    /*
     ** Perform an orderly shutdown of the server channel and all of its associated resources.
     */
    public void stop() {

        System.out.println("ServerChannelLayer[" + (serverClientId * 100) + "] stop()");

        //FIXME: does not work with InitiatorServer
        serverWorkHandler.stop();
        digestThreadPool.stop();

        /*
         */
        try {
            serverChannel.close();
        } catch (IOException io_ex) {
            //FIXME: handle this
            LOG.info("Unable to close server socket: " + serverConnTransactionId + " " + io_ex.getMessage());
        }

        /*
         ** Shutdown the AsynchronousChannelGroup and wait for it to cleanup
         */
        serverCbThreadpool.shutdown();

        try {
            boolean shutdown = serverCbThreadpool.awaitTermination(CHAN_TIMEOUT, TimeUnit.MILLISECONDS);
            if (!shutdown) {
                LOG.info("Wait for threadpool shutdown timed out: " + serverConnTransactionId);
            }
        } catch (InterruptedException int_ex) {
            LOG.info("Wait for threadpool shutdown failed: " + serverConnTransactionId + " " + int_ex.getMessage());
        }

        System.out.println("ServerChannelLayer[" + (serverClientId * 100) + "] stop() finished");
    }

    public void run() {

        LOG.info("ServerChannelLayer(" + serverClientId + ") start " + Thread.currentThread().getName());

        try {
            serverCbThreadpool = AsynchronousChannelGroup.withFixedThreadPool(WORK_QUEUE_SIZE, Executors.defaultThreadFactory());
        } catch (IOException io_ex) {
            //FIXME: handle this
            LOG.info("Unable to create server threadpool " + io_ex.getMessage());
            return;
        }

        try {
            serverChannel = AsynchronousServerSocketChannel.open(serverCbThreadpool);

            InetSocketAddress serverAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), portNum);

            LOG.info("Server run(" + serverClientId + "): server: " + serverAddr);

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
            LOG.info("Server run(" + serverClientId + "): waiting on accept");

            try {
                AsynchronousSocketChannel clientChan = clientAcceptChan.get();

                LOG.info("Server run(" + serverClientId + "): accepted");

                if (!serverWorkHandler.startNewConnection(clientChan, sslContext)) {
                    /*
                    FIXME: currently this only happens if the load balancer was unable to allocate a connection pool
                    from the unreserved normal pool, and also unable to allocate a connection pool from the unreserved
                    blocking pool. This probably shouldn't be passed up to us via a boolean return type, and depending
                    on the solution we design, might want to be handled at a lower layer.
                     */
                }
            } catch (ExecutionException | InterruptedException ex) {
                LOG.info("Server run(): accept error: " + ex.getMessage());
                exitThreads = true;
            }
        }

        LOG.info("ServerChannelLayer(" + serverClientId + ") exit " + Thread.currentThread().getName());
    }

    private KeyManager[] createKeyManagers(String filepath, String keystorePassword, String keyPassword) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        InputStream keyStoreIS = new FileInputStream(filepath);
        try {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        } finally {
            if (keyStoreIS != null) {
                keyStoreIS.close();
            }
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    private TrustManager[] createTrustManagers(String filepath, String keystorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        InputStream trustStoreIS = new FileInputStream(filepath);
        try {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        } finally {
            if (trustStoreIS != null) {
                trustStoreIS.close();
            }
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }

}
