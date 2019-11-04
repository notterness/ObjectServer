package com.oracle.athena.webserver.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WriteConnThread implements Runnable {

    private final int WORK_QUEUE_SIZE = 10;

    private BlockingQueue<WriteConnection> workQueue;

    private Thread connWriteThread;
    private boolean threadExit;

    private int writeThreadId;

    public WriteConnThread(int threadId) {

        writeThreadId = threadId;
        threadExit = false;
    }

    public void start() {
        workQueue = new LinkedBlockingQueue<>(WORK_QUEUE_SIZE);

        connWriteThread = new Thread(this);
        connWriteThread.start();
    }

    public void stop() {
        threadExit = true;

        try {
            connWriteThread.join(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("Unable to join client write thread: " + int_ex.getMessage());
        }
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
        System.out.println("writeConnThread(" + writeThreadId + ") start");
        try {
            WriteConnection writeConnection;

            while (!threadExit) {
                if ((writeConnection = workQueue.poll(1000, TimeUnit.MILLISECONDS)) != null) {
                    // Perform write to this socket
                    if (writeConnection != null) {
                        writeConnection.writeAvailableData();
                    } else {
                        System.out.println("writeConnThread() no write data ");
                    }
                } else {
                    // Check when last heartbeat was sent
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("writeConnThread(" + writeThreadId + ") exit");
    }

}
