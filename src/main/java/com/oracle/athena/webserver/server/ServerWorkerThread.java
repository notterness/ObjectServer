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

    private int maxQueueSize;

    private int threadId;

    private MemoryManager memoryManager;

    private BlockingQueue<ConnectionState> workQueue;

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
        workQueue = new LinkedBlockingQueue<>(maxQueueSize);

        this.memoryManager = memoryManager;

        threadId = identifier;

        threadExit = false;
    }

    void start() {
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


    public void run() {

        System.out.println("ServerWorkerThread(" + threadId + ") start " + Thread.currentThread().getName());
        try {
            ConnectionState work;

            while (!threadExit) {
                if ((work = workQueue.poll(1000, TimeUnit.MILLISECONDS)) != null) {
                    // Perform read from this socket
                    work.stateMachine();
                } else {
                    // TODO: Should there be a health check for the processing thread?
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("ServerWorkerThread(" + threadId + ") exit " + Thread.currentThread().getName());
    }

}
