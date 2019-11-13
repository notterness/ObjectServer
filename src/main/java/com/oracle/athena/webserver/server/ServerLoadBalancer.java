package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.ConnectionStatePool;
import com.oracle.athena.webserver.connectionstate.WebServerConnState;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.nio.channels.AsynchronousSocketChannel;

/*
 ** This distributes the accepted channels amongst the available worker threads. The goal is to
 ** evenly (or at least make a best effort) spread the connections to the worker threads. The
 ** problem is that the amount of work per connection is not really known until the header is
 ** parsed and then there additional detail about what needs to be done.
 */
class ServerLoadBalancer {

    ServerWorkerThread[] threadPool;

    MemoryManager memoryManager;

    int workerThreads;
    int maxQueueSize;
    int lastQueueUsed;

    int serverBaseId;
    private ConnectionStatePool<WebServerConnState> connPool;

    /*
     ** The queueSize is the maximum number of outstanding connections a thread can manage
     **   before an error needs to be returned that the thread cannot handle any more work. When
     **   that happens, an error needs to be returned to the client asking it to try again later.
     ** The numWorkerThreads is how many threads are available to accept connections and perform
     **   work on that connection.
     **
     ** queueSize * numWorkerThreads is the maximum number of concurrent client connections that can
     **   be handled by the server.
     */
    ServerLoadBalancer(final int queueSize, final int numWorkerThreads, MemoryManager memoryManager, int serverClientId) {

        workerThreads = numWorkerThreads;
        maxQueueSize = queueSize;
        serverBaseId = serverClientId;

        this.memoryManager = memoryManager;

        System.out.println("ServerLoadBalancer[" + serverClientId + "] workerThreads: " + workerThreads + " maxQueueSize: " + maxQueueSize);

    }

    void start() {
        threadPool = new ServerWorkerThread[workerThreads];

        for (int i = 0; i < workerThreads; i++) {
            ServerWorkerThread worker = new ServerWorkerThread(maxQueueSize, memoryManager,
                    (serverBaseId + i));
            worker.start();
            threadPool[i] = worker;
        }

        connPool = new ConnectionStatePool<WebServerConnState>(workerThreads * maxQueueSize, serverBaseId);

        /*
        ** The following ugly code is due to the fact that you cannot create a object of generic type <T> within
        **   and generic class that uses <T>
         */
        WebServerConnState conn;
        for (int i = 0; i < (workerThreads * maxQueueSize); i++) {
            conn = new WebServerConnState(connPool, (serverBaseId + i + 1));

            conn.start();

            connPool.freeConnectionState(conn);
        }

        lastQueueUsed = 0;
    }

    void stop() {
        for (int i = 0; i < workerThreads; i++) {
            ServerWorkerThread worker = threadPool[i];
            threadPool[i] = null;
            worker.stop();
        }

        // TODO: Wait for all the threads to stop and exit
    }

    /*
     ** The following is used to start a new Server read connection
     */
    boolean startNewConnection(final AsynchronousSocketChannel chan) {

        WebServerConnState work = connPool.allocConnectionState(chan);
        if (work == null) {
            return false;
        }

        return addWorkToThread(work);
    }

    boolean addWorkToThread(ConnectionState work) {

        // Find the queue with the least amount of work
        int currQueue = lastQueueUsed;

        while (true) {
            try {
                int queueCap = threadPool[currQueue].getCurrentWorkload();
                if ((maxQueueSize - queueCap) < maxQueueSize) {
                    System.out.println("addReadWork(" + (serverBaseId + currQueue) + "): currQueue: " +
                            currQueue + " queueCap: " + queueCap);

                    /*
                    ** Need to assign the thread this ConnectionState is going to run on prior to adding it
                    **   to the execution queue for the worker thread.
                     */
                    work.assignWorkerThread(threadPool[currQueue]);
                    work.addToWorkQueue(false);

                    lastQueueUsed = currQueue + 1;
                    if (lastQueueUsed == workerThreads) {
                        lastQueueUsed = 0;
                    }
                    break;
                } else {
                    System.out.println("addReadWork(): no capacity: " + queueCap + " maxQueueSize: " + maxQueueSize);

                    currQueue++;
                    if (currQueue == workerThreads) {
                        currQueue = 0;
                    }

                    if (currQueue == lastQueueUsed) {
                        Thread.sleep(100);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }

        return true;
    }
}
