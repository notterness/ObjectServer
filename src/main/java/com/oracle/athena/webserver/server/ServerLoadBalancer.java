package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BlockingConnectionStatePool;
import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.connectionstate.ConnectionStatePool;
import com.oracle.athena.webserver.connectionstate.WebServerConnState;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;

import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 ** This distributes the accepted channels amongst the available worker threads. The goal is to
 ** evenly (or at least make a best effort) spread the connections to the worker threads. The
 ** problem is that the amount of work per connection is not really known until the header is
 ** parsed and then there additional detail about what needs to be done.
 */
public class ServerLoadBalancer {

    private static final Logger LOG = LoggerFactory.getLogger(ServerLoadBalancer.class);

    private final static int MAX_WORKER_THREADS = 100;
    private final static int DIGEST_THREAD_ID_OFFSET = 500;
    private final static int ENCRYPT_THREAD_ID_OFFSET = 550;

    public final static int RESERVED_CONN_COUNT = 2;
    protected final static int NUMBER_COMPUTE_THREADS = 1;

    protected final WebServerFlavor flavor;
    protected final ServerWorkerThread[] threadPool;
    protected final MemoryManager memoryManager;
    protected final int numWorkerThreads;
    protected final int maxQueueSize;
    protected int lastQueueUsed;
    protected final int serverBaseId;
    protected final ServerDigestThreadPool digestThreadPool;
    protected final EncryptThreadPool encryptThreadPool;

    private ConnectionStatePool<WebServerConnState> connPool;
    private ConnectionStatePool<WebServerConnState> reservedBlockingConnPool;

    /*
     ** The queueSize is the maximum number of outstanding connections a thread can manage
     **   before an error needs to be returned that the thread cannot handle any more work. When
     **   that happens, an error needs to be returned to the client asking it to try again later.
     ** The numWorkerThreads is how many threads are available to accept connections and perform
     **   work on that connection.
     **
     ** queueSize * numWorkerThreads is the maximum number of concurrent client connections that can
     **   be handled by the server.
     **
     ** THREAD POOLS
     **   There a currently (only three are implemented as of this moment) four thread pool that are
     **     used to process a connection.
     **       Worker Thread Pool - A worker thread can process multiple connections at the same time. This is
     **         the driver of the work required to handle an operation (i.e. a PUT). The connection moves
     **         through its various stages under the direction of different pipeline managers. For example, there
     **         is an HttpParsePipelineMgr to handle reading in the HTTP request header and extracting certain
     **         pieces of information out of it. There is a ContentReadPipelineMgr to handle the PUT operations
     **         reading of content data and then moving that data through various steps until it is written to
     **         the Storage Server.
     **
     **       ServerDigestThreadPool - This is a number of ComputeThreads that are used to compute the Md5
     **         digest for a buffer. Since the ServerDigestThread(s) are CPU intensive, there is a limited
     **         number that are shared between all connections.
     **
     **       EncryptThreadPool - This is a number of ComputeThreads that are used to encrypt the data within
     **         a buffer and place the encrypted data into the Chunk buffer that is used to write out to the
     **         StorageServer. Since the EncryptThread(s) are CPU intensive, there is a limited
     **         number that are shared between all connections.
     */
    public ServerLoadBalancer(final WebServerFlavor flavor, final int queueSize, final int numWorkerThreads, MemoryManager memoryManager, int serverClientId) {

        this.flavor = flavor;

        /*
        ** Limit the maximum number of worker threads to 100. That is still probably too many
        **   threads to be running when most systems have far fewer than 100 cores available
        **   to run workloads on.
         */
        if (numWorkerThreads > MAX_WORKER_THREADS) {
            LOG.info("ServerLoadBalancer[" + serverClientId + "] workerThreads apped at: " + MAX_WORKER_THREADS);
            this.numWorkerThreads = MAX_WORKER_THREADS;
        } else {
            this.numWorkerThreads = numWorkerThreads;
        }
        this.maxQueueSize = queueSize;
        this.serverBaseId = serverClientId;
        this.memoryManager = memoryManager;
        LOG.info("ServerLoadBalancer[" + serverClientId + "] workerThreads: " + this.numWorkerThreads + " maxQueueSize: " + maxQueueSize);

        /*
        ** To uniquely identify the ServerDigestThread(s) and EncryptThread(s), the base Id range
        **   will start at +500 and +550 from the serverBaseId. There will never be more than
        **   100 ServerThreads as that is not a very efficient use of worker threads when each one
        **   is capable of handling hundreds of connections.
         */
        this.digestThreadPool = new ServerDigestThreadPool(NUMBER_COMPUTE_THREADS, (this.serverBaseId + DIGEST_THREAD_ID_OFFSET));
        this.encryptThreadPool = new EncryptThreadPool(NUMBER_COMPUTE_THREADS, (this.serverBaseId + ENCRYPT_THREAD_ID_OFFSET));

        this.threadPool = new ServerWorkerThread[this.numWorkerThreads];
    }

    void start() {

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

        /*
        ** The following ugly code is due to the fact that you cannot create a object of generic type <T> within
        **   and generic class that uses <T>
         */
        WebServerConnState conn;
        for (int i = 0; i < (numWorkerThreads * maxQueueSize); i++) {
            conn = new WebServerConnState(flavor, connPool, (serverBaseId + i + 1));
            conn.start();
            connPool.freeConnectionState(conn);
        }

        // also populate the reserved connection pool
        int startingId = serverBaseId + (numWorkerThreads * maxQueueSize) + 1;
        for (int i = 0; i < RESERVED_CONN_COUNT; i++) {
            conn = new WebServerConnState(flavor, reservedBlockingConnPool, (startingId + i));
            conn.start();
            reservedBlockingConnPool.freeConnectionState(conn);
        }

        lastQueueUsed = 0;
    }

    void stop() {

        digestThreadPool.stop();
        encryptThreadPool.stop();

        for (int i = 0; i < numWorkerThreads; i++) {
            ServerWorkerThread worker = threadPool[i];
            threadPool[i] = null;
            worker.stop();
        }
    }

    /*
     ** The following is used to start a new Server read connection. In the event there are no available connections,
     **   a special connection will be allocated from a different pool that will return an error of
     **   TOO_MANY_REQUESTS_429 after reading in the HTTP headers.
     */
    //FIXME: avoid boolean return types in favor of void with exception handling, if all the return type means is
    //"completed without surprises."  Throw exceptions instead of returning false.
    boolean startNewConnection(final AsynchronousSocketChannel chan) {

        WebServerConnState work = connPool.allocConnectionState(chan);
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

    //FIXME: avoid boolean return types in favor of void with exception handling, if all the return type means is
    //"completed without surprises."  Throw exceptions instead of returning false.
    protected boolean addWorkToThread(ConnectionState work) {

        //FIXME: do least amount of work processing per comment
        // Find the queue with the least amount of work
        int currQueue = lastQueueUsed;

        while (true) {
            try {
                int queueCap = threadPool[currQueue].getCurrentWorkload();
                if ((maxQueueSize - queueCap) < maxQueueSize) { //FIXME: simplify condition
                    LOG.info("addReadWork(" + (serverBaseId + currQueue) + "): currQueue: " +
                            currQueue + " queueCap: " + queueCap);

                    /*
                    ** Need to assign the thread this ConnectionState is going to run on prior to adding it
                    **   to the execution queue for the worker thread.
                     */
                    work.assignWorkerThread(threadPool[currQueue]);
                    work.addToWorkQueue(false);

                    lastQueueUsed = currQueue + 1;
                    if (lastQueueUsed == numWorkerThreads) {
                        lastQueueUsed = 0;
                    }
                    break;
                } else {
                    LOG.info("addReadWork(): no capacity: " + queueCap + " maxQueueSize: " + maxQueueSize);

                    currQueue++;
                    if (currQueue == numWorkerThreads) {
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
