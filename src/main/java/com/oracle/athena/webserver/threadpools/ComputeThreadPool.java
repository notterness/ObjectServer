package com.oracle.athena.webserver.threadpools;

import com.oracle.athena.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

abstract class ComputeThreadPool {

    private static final Logger LOG = LoggerFactory.getLogger(ComputeThreadPool.class);

    // TODO: Fix the based upon the number of connections
    private final static int COMPUTE_MAX_QUEUE_SIZE = 2;

    private final static int MAX_EXEC_WORK_LOOP_COUNT = 1;

    protected final int threadCount;
    protected final int baseThreadId;
    protected final ComputeThread[] computeThreads;
    protected final BlockingQueue<Operation> workQueue;

    private final ReentrantLock queueMutex;
    private final Condition queueSignal;
    private boolean workQueued;


    ComputeThreadPool(final int threadCount, final int baseThreadId){
        this.threadCount = threadCount;
        this.baseThreadId = baseThreadId;
        this.computeThreads = new ComputeThread[this.threadCount];

        workQueue = new LinkedBlockingQueue<>(COMPUTE_MAX_QUEUE_SIZE);

        LOG.error("ComputeThreadPool[" + this.baseThreadId + "] threadCount: " + this.threadCount);

        queueMutex = new ReentrantLock();
        queueSignal = queueMutex.newCondition();
        workQueued = false;
    }

    void start () {
        for (int i = 0; i < threadCount; i++) {
            computeThreads[i] = new ComputeThread(this, baseThreadId + i);
            computeThreads[i].start();
        }
    };

    void stop () {
        for (int i = 0; i < threadCount; i++) {
            computeThreads[i].stop();
        }
    }

    /*
     */
    void addComputeWorkToThread(final Operation computeOperation) {

        queueMutex.lock();
        try {
            boolean isOnWorkQueue = computeOperation.isOnWorkQueue();

            if (!isOnWorkQueue) {
                /*
                 ** Only log if it is not on the work queue already
                 */
                LOG.info("requestId[] addToWorkQueue() operation(" +
                        computeOperation.getOperationType() + ") onExecutionQueue: " +
                        isOnWorkQueue);

                if (!workQueue.offer(computeOperation)) {
                    LOG.error("requestId[] addToWorkQueue() unable to add");
                } else {
                    computeOperation.markAddedToQueue(false);
                    queueSignal.signal();

                    workQueued = true;
                }
            }
        } finally {
            queueMutex.unlock();
        }
    }

    /*
    **
     */
    boolean executeComputeWork() {
        List<Operation> operationsToRun = new ArrayList<>();

        try {
            queueMutex.lock();
            try {
                if (workQueued || queueSignal.await(100, TimeUnit.MILLISECONDS)) {
                    int drainedCount = workQueue.drainTo(operationsToRun, MAX_EXEC_WORK_LOOP_COUNT);
                    for (Operation operation : operationsToRun) {
                        operation.markRemovedFromQueue(false);
                    }

                    /*
                     ** Make sure this is only modified with the queueMutex lock. If there might still be
                     **   elements on the queue, do not clear the workQueued flag.
                     */
                    if (drainedCount != MAX_EXEC_WORK_LOOP_COUNT) {
                        workQueued = false;
                    }
                }
            } finally {
                queueMutex.unlock();
            }

            for (Operation operation : operationsToRun) {
                LOG.info("Compute requestId[" + operation.getRequestId() + "] operation(" + operation.getOperationType() + ") execute");
                operation.execute();
            }

        } catch (InterruptedException int_ex) {
            /*
             ** Need to close out this request since something serious has gone wrong.
             */
            return false;
        }

        return true;
    }

}
