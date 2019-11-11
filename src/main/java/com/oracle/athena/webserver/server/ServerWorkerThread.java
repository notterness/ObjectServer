package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.BufferStatePool;
import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.http.BuildHttpResult;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.util.concurrent.BlockingQueue;
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

    private boolean threadExit;
    private Thread connThread;

    ServerWorkerThread(final int queueSize, MemoryManager memoryManager, final int identifier) {

        maxQueueSize = queueSize;

        this.memoryManager = memoryManager;

        threadId = identifier;

        threadExit = false;
    }

    void start() {

        workQueue = new LinkedBlockingQueue<>(maxQueueSize);
        timedWaitQueue = new LinkedBlockingQueue<>(maxQueueSize);

        bufferStatePool = new BufferStatePool(ConnectionState.MAX_OUTSTANDING_BUFFERS * maxQueueSize,
                memoryManager);

        resultBuilder = new BuildHttpResult();

        writeThread = new WriteConnThread(threadId);
        writeThread.start();

        connThread = new Thread(this);
        connThread.start();
    }

    void stop() {

        threadExit = true;

        writeThread.stop();

        try {
            connThread.join(1000);
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
    public boolean put(final ConnectionState work) {
        if (workQueue.remainingCapacity() == 0) {
            System.out.println("ServerWorkerThread() put connStateId [%d] out of space threadId: " + threadId + " " + Thread.currentThread().getName());
            return false;
        }
        try {
            String outStr;

            outStr = String.format("ServerWorkerThread(%d) put connStateId [%d]  state %s",
                    this.threadId, work.getConnStateId(), work.getState().toString());
            System.out.println(outStr);

            workQueue.put(work);
        } catch (InterruptedException int_ex) {
            System.out.println("ServerWorkerThread() connStateId [%d] put failed threadId: " + threadId + " " + Thread.currentThread().getName());
            return false;
        }

        return true;
    }

    public boolean addToTimedQueue(final ConnectionState work) {
        if (timedWaitQueue.remainingCapacity() == 0) {
            System.out.println("ServerWorkerThread() addToTimedQueue connStateId [%d] out of space threadId: " + threadId + " " + Thread.currentThread().getName());
            return false;
        }
        try {
            String outStr;

            outStr = String.format("ServerWorkerThread(%d) addToTimedQueue connStateId [%d]  state %s",
                    this.threadId, work.getConnStateId(), work.getState().toString());
            System.out.println(outStr);

            timedWaitQueue.put(work);
        } catch (InterruptedException int_ex) {
            System.out.println("ServerWorkerThread() connStateId [%d] addToTimedQueue failed threadId: " + threadId + " " + Thread.currentThread().getName());
            return false;
        }

        return true;
    }

    /*
    ** Remove this element from the timedWaitQueue
     */
    public void removeFromTimedWaitQueue(final ConnectionState work) {
        try {
            if (!timedWaitQueue.remove(work)) {
                System.out.println("ConnectionState[" + work.getConnStateId() + "] not on timedWaitQueue");
            }
        } catch (NullPointerException ex) {
            ex.printStackTrace();
        }
    }


    public void run() {

        System.out.println("ServerWorkerThread(" + threadId + ") start " + Thread.currentThread().getName());
        try {
            ConnectionState work;
            int workLoopCount;

            while (!threadExit) {
                workLoopCount = 0;
                while (((work = workQueue.poll(100, TimeUnit.MILLISECONDS)) != null) &&
                        (workLoopCount < MAX_EXEC_WORK_LOOP_COUNT)) {
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
                    if (work.hasWaitTimeElapsed()) {
                        timedWaitQueue.remove(work);
                        work.markRemovedFromQueue(true);
                        work.stateMachine();
                    } else {
                        break;
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("ServerWorkerThread(" + threadId + ") exit " + Thread.currentThread().getName());
    }

}
