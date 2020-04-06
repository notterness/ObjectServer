package com.webutils.webserver.threadpools;

import com.webutils.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ComputeThreadPool {

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


    public ComputeThreadPool(final int threadCount, final int baseThreadId) {
        this.threadCount = threadCount;
        this.baseThreadId = baseThreadId;
        this.computeThreads = new ComputeThread[this.threadCount];

        workQueue = new LinkedBlockingQueue<>(COMPUTE_MAX_QUEUE_SIZE);

        queueMutex = new ReentrantLock();
        queueSignal = queueMutex.newCondition();
        workQueued = false;
    }

    public void start() {
        LOG.info("ComputeThreadPool[" + this.baseThreadId + "] start() threadCount: " + this.threadCount);
        for (int i = 0; i < threadCount; i++) {
            computeThreads[i] = new ComputeThread(this, baseThreadId + i);
            computeThreads[i].start();
        }
    }

    public void stop() {
        LOG.info("ComputeThreadPool[" + this.baseThreadId + "] stop()");
        for (int i = 0; i < threadCount; i++) {
            computeThreads[i].stop();
            computeThreads[i] = null;
        }
    }

    /*
     ** This adds the Compute work to the general queue. The queue is shared by all of the compute worker threads
     **   so that as they complete a piece of work, they can pick up what is next on the queue. This is done instead
     **   of trying to figure out a fairness algorithm based upon work that has been assigned to a thread.
     */
    public void addComputeWorkToThread(final Operation computeOperation) {

        queueMutex.lock();
        try {
            boolean isOnWorkQueue = computeOperation.isOnWorkQueue();

            if (!isOnWorkQueue) {
                /*
                 ** Only log if it is not on the work queue already
                 */
                LOG.info("requestId[] addComputeWorkToThread() operation(" +
                        computeOperation.getOperationType() + ") onExecutionQueue: " +
                        isOnWorkQueue);

                if (!workQueue.offer(computeOperation)) {
                    LOG.error("requestId[] addComputeWorkToThreadn() unable to add");
                } else {
                    computeOperation.markAddedToQueue(false);

                    workQueued = true;
                    queueSignal.signal();
                }
            }
        } finally {
            queueMutex.unlock();
        }
    }

    /*
     ** This is what is called by the different compute worker threads to actually perform the work.
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

    /*
     ** This is to remove an item from the queue as there is a race condition where a compute operation can be working
     **   on a something (i.e. computing the Md5 digest for a buffer) when an additional buffer arrives that can be
     **   worked on. If the buffer that arrives is the last one for the client object, the compute operation
     **   can complete all of its work and still be on the queue to be run.
     ** So, the last thing a compute operation needs to do before finishing it to make sure it is not on the
     **   execute queue.
     */
    public void removeFromComputeThread(final Operation computeOperation) {
        try {
            queueMutex.lock();
            workQueue.remove(computeOperation);
            computeOperation.markRemovedFromQueue(false);
        } finally {
            queueMutex.unlock();
        }
    }

}
