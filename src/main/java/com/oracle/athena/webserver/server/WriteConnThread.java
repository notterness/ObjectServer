package com.oracle.athena.webserver.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteConnThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(WriteConnThread.class);

    private final int WORK_QUEUE_SIZE = 10;

    private BlockingQueue<WriteConnection> workQueue;

    private Thread connWriteThread;

    private volatile boolean stopReceived;
    private int writeThreadId;

    public WriteConnThread(int threadId) {
        writeThreadId = threadId;
        workQueue = new LinkedBlockingQueue<>(WORK_QUEUE_SIZE);

        stopReceived = false;
    }

    public void start() {
        connWriteThread = new Thread(this);
        connWriteThread.start();
    }


    public boolean stop(long timeout, TimeUnit unit) {
        stopReceived = true;
        try {
            connWriteThread.join(1000);
        } catch (InterruptedException e) {
            LOG.info("Unable to join client write thread: " + e.getMessage());
        }
        return false;
    }

    public boolean writeData(WriteConnection writeConnection, WriteCompletion completion) {
        writeConnection.writeData(completion);
        workQueue.add(writeConnection);

        return true;
    }


    /*
     **
     ** TODO: The single thread to handle all the writes doesn't really work since the writes
     **   per connection need to be ordered and a write needs to complete before the next
     **   one is allowed to start. Something more like a map that states a connection has
     **   work pending and then calling the writeAvailableData() might be a better solution.
     */
    public void run() {
        LOG.info("writeConnThread(" + writeThreadId + ") start");
        try {
            WriteConnection writeConnection;

            while (!stopReceived) {
                if ((writeConnection = workQueue.poll(1000, TimeUnit.MILLISECONDS)) != null) {
                    // Perform write to this socket
                    if (writeConnection != null) {
                        writeConnection.writeAvailableData();
                    } else {
                        LOG.info("writeConnThread() no write data ");
                    }
                } else {
                    // Check when last heartbeat was sent
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOG.info("writeConnThread(" + writeThreadId + ") exit");
    }

}
