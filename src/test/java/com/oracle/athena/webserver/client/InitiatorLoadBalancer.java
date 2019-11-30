package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.ConnectionStatePool;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.ServerDigestThreadPool;
import com.oracle.athena.webserver.server.ServerLoadBalancer;
import com.oracle.athena.webserver.server.ServerWorkerThread;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;

import java.nio.channels.AsynchronousSocketChannel;

class InitiatorLoadBalancer extends ServerLoadBalancer {

    private ConnectionStatePool<ClientConnState> connPool;

    InitiatorLoadBalancer(final int queueSize, final int numWorkerThreads, MemoryManager memoryManager, int serverClientId ){

        super(WebServerFlavor.INTEGRATION_TESTS, queueSize, numWorkerThreads, memoryManager, serverClientId);

        System.out.println("InitiatorLoadBalancer[" + serverClientId + "] workerThreads: " + numWorkerThreads + " maxQueueSize: " + maxQueueSize);
    }

    void start() {
        digestThreadPool.start();

        for (int i = 0; i < numWorkerThreads; i++) {
            ServerWorkerThread worker = new ServerWorkerThread(maxQueueSize, memoryManager,
                    (serverBaseId + i), digestThreadPool);
            worker.start();
            threadPool[i] = worker;
        }

        lastQueueUsed = 0;

        connPool = new ConnectionStatePool<>(numWorkerThreads * maxQueueSize);

        /*
         ** The following ugly code is due to the fact that you cannot create a object of generic type <T> within
         **   and generic class that uses <T>
         */
        ClientConnState conn;
        for (int i = 0; i < (numWorkerThreads * maxQueueSize); i++) {
            conn = new ClientConnState(connPool, (serverBaseId + i + 1));

            conn.start();

            connPool.freeConnectionState(conn);
        }
    }

    void stop() {

    }

    /*
     ** The following is used to register a Connection state object used to perform
     **   reads from the clients socket connection.
     **
     ** NOTE: This returns ConnectionState to allow the client to perform close operations on
     **   the connection during tests.
     */
    ConnectionState startNewClientReadConnection(final AsynchronousSocketChannel chan, final ClientDataReadCallback clientReadCb) {
        ClientConnState work = connPool.allocConnectionState(chan);
        if (work != null) {
            work.setClientReadCallback(clientReadCb);
            if (!addWorkToThread(work)) {
                connPool.freeConnectionState(work);
                work = null;
            }
        }

        return work;
    }

}
