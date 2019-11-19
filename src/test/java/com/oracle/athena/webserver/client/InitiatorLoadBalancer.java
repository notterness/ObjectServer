package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.ConnectionStatePool;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.ServerLoadBalancer;
import com.oracle.athena.webserver.server.ServerWorkerThread;

import java.nio.channels.AsynchronousSocketChannel;

class InitiatorLoadBalancer extends ServerLoadBalancer {

    private ConnectionStatePool<ClientConnState> connPool;

    InitiatorLoadBalancer(final int queueSize, final int numWorkerThreads, MemoryManager memoryManager, int serverClientId) {

        super(queueSize, numWorkerThreads, memoryManager, serverClientId);
    }

    void start() {
        threadPool = new ServerWorkerThread[workerThreads];

        for (int i = 0; i < workerThreads; i++) {
            ServerWorkerThread worker = new ServerWorkerThread(maxQueueSize, memoryManager,
                    (serverBaseId + i));
            worker.start();
            threadPool[i] = worker;
        }

        lastQueueUsed = 0;

        connPool = new ConnectionStatePool<>(workerThreads * maxQueueSize, serverBaseId);

        /*
         ** The following ugly code is due to the fact that you cannot create a object of generic type <T> within
         **   and generic class that uses <T>
         */
        ClientConnState conn;
        for (int i = 0; i < (workerThreads * maxQueueSize); i++) {
            conn = new ClientConnState(connPool, (serverBaseId + i + 1));

            conn.start();

            connPool.freeConnectionState(conn);
        }

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
