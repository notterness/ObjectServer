package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferState;
import com.oracle.athena.webserver.connectionstate.BufferStatePool;
import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.http.BuildHttpResult;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 ** This is the implementation for the worker thread that manages the state transitions for the ConnectionState
 */
public class ServerWorkerThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerWorkerThread.class);

    /*
    ** This is how many items to pull off the execution thread before going and checking the
    **   timed wait queue for any work.
     */
    private final int MAX_EXEC_WORK_LOOP_COUNT = 20;

    private int maxQueueSize;

    private int threadId;

    private MemoryManager memoryManager;

    private ServerDigestThreadPool digestThreadPool;

    private BlockingQueue<ConnectionState> workQueue;
    private BlockingQueue<ConnectionState> timedWaitQueue;

    private BufferStatePool bufferStatePool;

    /*
     ** When a write is needed, the request is queued up to the writeThread to
     **   take place in the background
     */
    private WriteConnThread writeThread;

    /*
     ** For now create a BuildHttpResult per worker thread
     */
    private BuildHttpResult resultBuilder;

    /*
    ** Mutex to protect the addition and removal from the work and timed queues
     */
    private final ReentrantLock queueMutex;
    private final Condition queueSignal;
    private boolean workQueued;


    private volatile boolean stopReceived;
    private final CountDownLatch countDownLatch;

    public ServerWorkerThread(final int queueSize, final MemoryManager memoryManager, final int threadId,
                              final ServerDigestThreadPool digestThreadPool) {

        maxQueueSize = queueSize;

        this.memoryManager = memoryManager;
        this.threadId = threadId;
        this.digestThreadPool = digestThreadPool;

        queueMutex = new ReentrantLock();
        queueSignal = queueMutex.newCondition();
        workQueued = false;

        // TODO: Need to fix the queue size to account for reserved connections
        workQueue = new LinkedBlockingQueue<>(maxQueueSize * 2);
        timedWaitQueue = new LinkedBlockingQueue<>(maxQueueSize * 2);
        bufferStatePool = new BufferStatePool(ConnectionState.MAX_OUTSTANDING_BUFFERS * maxQueueSize,
                memoryManager);
        resultBuilder = new BuildHttpResult();
        writeThread = new WriteConnThread(threadId);
        countDownLatch = new CountDownLatch(1);
    }

    // FIXME CTSA: expose timeout and units if applicable
    public void stop() {
        stopReceived = true;
        writeThread.stop(1000, TimeUnit.MILLISECONDS);
        try {
            countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException int_ex) {
            LOG.info("Server worker thread[" + threadId + "] failed: " + int_ex.getMessage());
        }

        /*
        ** Code to check that the BufferStatePool does not have BufferState objects on its in use list
        **   when the ServerWorkerThread is shutting down.
         */
        bufferStatePool.showInUseBufferState(threadId);
     }

    /*
     ** This function is used to return what the thread thinks its current workload is. This is used to determine
     ** if new work should be added to this thread or if it is already too busy.
     */
    int getCurrentWorkload() {
        return workQueue.remainingCapacity();
    }

    /*
     ** The ConnectionState objects need a way to access the BufferStatePool that is
     **   associated with a particular worker thread.
     */
    public BufferStatePool getBufferStatePool() {
        return bufferStatePool;
    }

    /*
     **
     */
    public WriteConnThread getWriteThread() {
        return writeThread;
    }

    /*
     **
     */
    public BuildHttpResult getResultBuilder() {
        return resultBuilder;
    }

    /*
     ** This checks to see if there is space in the queue and then adds the
     ** ConnectionState object if there is. It returns false if there is no
     ** space currently in the queue.
     */
    public void addToWorkQueue(final ConnectionState work) {
        queueMutex.lock();
        try {
            LOG.info("ConnectionState[" + work.getConnStateId() + "] addToWorkQueue() onExecutionQueue: " + work.isOnWorkQueue() +
                    " onTimedWaitQueue: " + work.isOnTimedWaitQueue());

            if (!work.isOnWorkQueue()) {
                if (work.isOnTimedWaitQueue()) {
                    if (timedWaitQueue.remove(work)) {
                        work.markRemovedFromQueue(true);
                    } else {
                        LOG.error("ConnectionState[" + work.getConnStateId() + "] addToWorkQueue() not on timedWaitQueue");
                    }
                }

                if (!workQueue.offer(work)) {
                    LOG.error("ConnectionState[" + work.getConnStateId() + "] addToWorkQueue() unable to add");
                } else {
                    work.markAddedToQueue(false);
                    queueSignal.signal();

                    workQueued = true;
                }
            }
        } finally {
            queueMutex.unlock();
        }
    }

    public void addToDelayedQueue(final ConnectionState work) {
        queueMutex.lock();
        try {
            LOG.info("ConnectionState[" + work.getConnStateId() + "] addToDelayedQueue() onExecutionQueue: " + work.isOnWorkQueue() +
                    " onTimedWaitQueue: " + work.isOnTimedWaitQueue());

            if (!work.isOnWorkQueue() && !work.isOnTimedWaitQueue()) {
                if (!timedWaitQueue.offer(work)) {
                    LOG.error("ConnectionState[" + work.getConnStateId() + "] addToWorkQueue() unable to add");
                } else {
                    work.markAddedToQueue(true);
                }
            }
        } finally {
            queueMutex.unlock();
        }
    }

    /*
    ** This removes the connection from whichever queue it is on. It emits a debug statement if it is not on
    **   the expected queue.
     */
    public void removeFromQueue(final ConnectionState work) {
        queueMutex.lock();
        try {
            if (workQueue.remove(work)) {
                LOG.info("ConnectionState[" + work.getConnStateId() + "] removeFromQueue() workQueue");
                work.markRemovedFromQueue(false);
            } else if (timedWaitQueue.remove(work)) {
                LOG.info("ConnectionState[" + work.getConnStateId() + "] removeFromQueue() timeWaitQueue");
                work.markRemovedFromQueue(true);
            } else {
                LOG.info("ConnectionState[" + work.getConnStateId() + "] removeFromQueue() not on any queue");
            }
        } finally {
            queueMutex.unlock();
        }
    }

    /*
    ** This is a debug tool to determine why items that are supposed to be on the queue are not executing.
     */
    public void dumpWorkerThreadQueues() {
        ConnectionState connState;
        Iterator<ConnectionState> iter;

        LOG.error("Delayed Queue");
        iter = timedWaitQueue.iterator();
        iter = workQueue.iterator();
        while (iter.hasNext()) {
            connState = iter.next();
            iter.remove();
            LOG.error("  DQ - ConnectionState[" + connState.getConnStateId() + "]");
        }
        LOG.error("Work Queue");
        iter = workQueue.iterator();
        while (iter.hasNext()) {
            connState = iter.next();
            iter.remove();
            LOG.error("  WQ - ConnectionState[" + connState.getConnStateId() + "]");
        }
    }

    /*
    ** This is the method that does the actual work for this thread. It handles multiple
    **   connections at once.
     */
    public void run() {

        LOG.info("ServerWorkerThread(" + threadId + ") start " + Thread.currentThread().getName());
        //FIXME: pool this?
        // every time this thread is created, create a second thread to handle the WriteConnection
        final Thread writeConnThread = new Thread(writeThread);
        try {
            ConnectionState work;

            // kick off the writeThread
            writeConnThread.start();
            while (!stopReceived) {
                ConnectionState connections[] = new ConnectionState[0];

                queueMutex.lock();
                try {
                    if (workQueued || queueSignal.await(100, TimeUnit.MILLISECONDS)) {
                        connections = workQueue.toArray(connections);
                        for (ConnectionState connState : connections) {
                            LOG.info("ConnectionState[" + connState.getConnStateId() + "] pulled from workQueue");
                            workQueue.remove(connState);
                            connState.markRemovedFromQueue(false);
                        }

                        /*
                        ** Make sure this is only modified with the queueMutex lock
                         */
                        workQueued = false;
                    }
                } finally {
                    queueMutex.unlock();
                }

                for (ConnectionState connState: connections)
                {
                    connState.stateMachine();
                }

                /*
                ** Check if there are ConnectionState that are on the timedWaitQueue and their
                **   wait time has elapsed. Currently, this is an ordered queue and everything
                **   on the queue has the same wait time so only the head element needs to be
                **   checked for the elapsed timeout.
                 */
                do {
                    queueMutex.lock();
                    try {
                        if ((work = timedWaitQueue.peek()) != null) {
                            if (work.hasWaitTimeElapsed()) {
                                LOG.info("ConnectionState[" + work.getConnStateId() + "] pulled from timedWaitQueue");
                                timedWaitQueue.remove(work);
                                work.markRemovedFromQueue(true);
                            } else {
                                work = null;
                            }
                        }
                    } finally {
                        queueMutex.unlock();
                    }

                    if (work != null) {
                        work.stateMachine();
                    }
                } while (work != null);
            }

            // FIXME CA: key  off of the value passed into stop
            writeConnThread.join(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("ServerWorkerThread(" + threadId + ") exit " + Thread.currentThread().getName());
        countDownLatch.countDown();
    }

    public boolean addServerDigestWork(BufferState bufferState) {
        boolean isQueued;

        isQueued = digestThreadPool.addDigestWorkToThread(bufferState);
        return isQueued;
    }

}
