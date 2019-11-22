package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferStatePool;
import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.http.BuildHttpResult;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 ** This is the implementation for the worker thread that manages the state transitions for the ConnectionState
 */
public class ServerWorkerThread implements Runnable {

    /*
    ** This is how many items to pull off the execution thread before going and checking the
    **   timed wait queue for any work.
     */
    private final int MAX_EXEC_WORK_LOOP_COUNT = 20;

    private int maxQueueSize;

    private int threadId;

    private MemoryManager memoryManager;

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
    private final Object queueMutex;


    private volatile boolean stopReceived;
    private final CountDownLatch countDownLatch;

    public ServerWorkerThread(final int queueSize, final MemoryManager memoryManager, final int threadId) {

        maxQueueSize = queueSize;

        this.memoryManager = memoryManager;
        this.threadId = threadId;
        queueMutex = new Object();
        workQueue = new LinkedBlockingQueue<>(maxQueueSize);
        timedWaitQueue = new LinkedBlockingQueue<>(maxQueueSize);
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
            System.out.println("Server worker thread[" + threadId + "] failed: " + int_ex.getMessage());
        }
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
     ** space or while waiting for space to add the element, the threads shuts
     ** down.
     */
    public boolean put(final ConnectionState work, final boolean delayedExecution) {

        System.out.println("ServerWorkerThread(" + this.threadId + ") put connStateId [" + work.getConnStateId() + "] delayed: " +
                delayedExecution);

        if (workQueue.contains(work)) {
            System.out.println("ConnectionState[" + work.getConnStateId() + "] addToWorkQueue() already on workQueue");
            return true;
        }

        if (delayedExecution) {
            /*
            ** If this ConnectionState is already on a queue, it cannot be added
            **   to the delayed execution queue
             */
            if (!timedWaitQueue.contains(work)) {
                work.markAddedToDelayedQueue();
                if (!timedWaitQueue.offer(work)) {
                    System.out.println("ConnectionState[" + work.getConnStateId() + "] addToWorkQueue(delayed) unable to add");
                    return false;
                }
            } else {
                System.out.println("ConnectionState[" + work.getConnStateId() + "] addToWorkQueue(delayed) already on queue");
            }
        } else {
            if (timedWaitQueue.contains(work)) {
                /*
                ** Need to remove this from the delayed queue and then put it on the execution
                **   queue
                 */
                System.out.println("ConnectionState[" + work.getConnStateId() + "] remove from delayed queue");
                work.markRemovedFromQueue(true);
                timedWaitQueue.remove(work);
            }

            if (!workQueue.offer(work)) {
                System.out.println("ConnectionState[" + work.getConnStateId() + "] addToWorkQueue() unable to add");
                return false;
            }
        }

        return true;
    }

    public void run() {

        System.out.println("ServerWorkerThread(" + threadId + ") start " + Thread.currentThread().getName());
        // every time this thread is created, create a second thread to handle the WriteConnection
        final Thread writeConnThread = new Thread(writeThread);
        try {
            ConnectionState work;
            int workLoopCount;
            // kick off the writeThread
            writeConnThread.start();
            while (!stopReceived) {
                workLoopCount = 0;
                while (((work = workQueue.poll(100, TimeUnit.MILLISECONDS)) != null) &&
                        (workLoopCount < MAX_EXEC_WORK_LOOP_COUNT)) {
                    System.out.println("ServerWorkerThread(" + threadId + ") [" + work.getConnStateId() + "]");

                    work.markRemovedFromQueue(false);
                    work.stateMachine();
                    workLoopCount++;
                }

                /*
                ** Check if there are ConnectionState that are on the timedWaitQueue and their
                **   wait time has elapsed. Currently, this is an ordered queue and everything
                **   on the queue has the same wait time so only the head element needs to be
                **   checked for the elapsed timeout.
                 */
                while ((work = timedWaitQueue.peek()) != null) {
                    System.out.println("ServerWorkerThread(" + threadId + ") [" + work.getConnStateId() + "] currTime: " +
                            System.currentTimeMillis());
                    if (work.hasWaitTimeElapsed()) {
                        timedWaitQueue.remove(work);
                        work.markRemovedFromQueue(true);
                        work.stateMachine();
                    } else {
                        break;
                    }
                }
            }
            // FIXME CA: key  off of the value passed into stop
            writeConnThread.join(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("ServerWorkerThread(" + threadId + ") exit " + Thread.currentThread().getName());
        countDownLatch.countDown();
    }

}
