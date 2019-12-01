package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.*;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.AsynchronousSocketChannel;
import java.security.KeyStore;
import java.security.SecureRandom;

/*
 ** This distributes the accepted channels amongst the available worker threads. The goal is to
 ** evenly (or at least make a best effort) spread the connections to the worker threads. The
 ** problem is that the amount of work per connection is not really known until the header is
 ** parsed and then there additional detail about what needs to be done.
 */
public class ServerSSLLoadBalancer extends ServerLoadBalancer {
    private static final Logger LOG = LoggerFactory.getLogger(ServerSSLLoadBalancer.class);

    private ConnectionStatePool<WebServerSSLConnState> connPool;
    private ConnectionStatePool<WebServerSSLConnState> reservedBlockingConnPool;

    public ServerSSLLoadBalancer(final WebServerFlavor flavor, final int queueSize, final int numWorkerThreads, MemoryManager memoryManager, int serverClientId) {
        super(flavor, queueSize, numWorkerThreads, memoryManager, serverClientId);

        LOG.info("SSLServerLoadBalancer[" + serverClientId + "] workerThreads: " + numWorkerThreads + " maxQueueSize: " + maxQueueSize);
    }

    @Override
    void start() {
        SSLContext sslContext;

        try {
            sslContext = SSLContext.getInstance("TLSv1.2");

            // TODO: figure out how to initialize with casper certs
            sslContext.init(createKeyManagers("./src/main/resources/server.jks", "athena", "athena"), createTrustManagers("./src/main/resources/trustedCerts.jks", "athena"), new SecureRandom());

        } catch (Exception e) {
            //TODO: Log that the SSL port did not start
            e.printStackTrace();
            return;
        }

        digestThreadPool.start();
        encryptThreadPool.start();

        for (int i = 0; i < numWorkerThreads; i++) {
            ServerWorkerThread worker = new ServerWorkerThread(maxQueueSize, memoryManager,
                    (serverBaseId + i), digestThreadPool, encryptThreadPool);
            worker.start();
            threadPool[i] = worker;
        }

        connPool = new ConnectionStatePool<>(numWorkerThreads * maxQueueSize);
        reservedBlockingConnPool = new BlockingConnectionStatePool<>(RESERVED_CONN_COUNT);

        //TODO: check if SSLEngine could not be instantiated

        /*
        ** The following ugly code is due to the fact that you cannot create a object of generic type <T> within
        **   and generic class that uses <T>
         */
        WebServerSSLConnState conn;
        for (int i = 0; i < (numWorkerThreads * maxQueueSize); i++) {
            conn = new WebServerSSLConnState(flavor, connPool, sslContext, (serverBaseId + i + 1));
            conn.start();
            connPool.freeConnectionState(conn);
        }
        // also populate the reserved connection pool
        int startingId = serverBaseId + (numWorkerThreads * maxQueueSize) + 1;
        for (int i = 0; i < RESERVED_CONN_COUNT; i++) {
            conn = new WebServerSSLConnState(flavor, reservedBlockingConnPool, sslContext, (startingId + i));
            conn.start();
            reservedBlockingConnPool.freeConnectionState(conn);
        }

        lastQueueUsed = 0;
    }


    /*
     ** The following is used to start a new Server read connection. In the event there are no available connections,
     **   a special connection will be allocated from a different pool that will return an error of
     **   TOO_MANY_REQUESTS_429 after reading in the HTTP headers.
     */
    //FIXME: avoid boolean return types in favor of void with exception handling, if all the return type means is
    //"completed without surprises."  Throw exceptions instead of returning false.
    @Override
    boolean startNewConnection(final AsynchronousSocketChannel chan) {

        WebServerSSLConnState work = connPool.allocConnectionState(chan);
        if (work == null) {
            /*
                This means the primary pool of connections has been depleted and one needs to be allocated from the
                special pool to return an error.
             */
            work = reservedBlockingConnPool.allocConnectionState(chan);
            if (work == null) {
                /*
                ** This means there was an exception while waiting to allocate the connection. Simply close the
                **    connection and give up.
                 */
                try {
                    chan.close();
                } catch (IOException io_ex) {
                    LOG.info("Unable to close");
                }

                return false;
            }

            LOG.info("Standard connection pool exhausted [" + work.getConnStateId() + "]");

            work.setOutOfResourceResponse();
        }

        return addWorkToThread(work);
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
