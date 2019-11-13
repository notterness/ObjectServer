package com.oracle.athena.webserver.connectionstate;

import com.oracle.athena.webserver.server.ClientDataReadCallback;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.LinkedBlockingQueue;

/*
 ** This class provides a method to manage the allocation of the ConnectionState objects
 **   to allow them to be obtained from a pool to prevent garbage collection.
 **
 ** numConnections is the total number allocated
 */
public class ConnectionStatePool<T> {

    private int allocatedConnectionStates;

    private int connId;

    private LinkedBlockingQueue<T> connPoolFreeList;

    /*
     ** The following are used to allow the LinkedBlockingQueue to not actually block
     **   by returning null when the queue is empty instead of waiting for an
     **   element to become available.
     */
    QueueMutex queueMutex;
    int freeConnCount;


    public ConnectionStatePool(final int numConnections, final int serverBaseId) {

        allocatedConnectionStates = numConnections;
        freeConnCount = 0;

        queueMutex = new QueueMutex();

        connPoolFreeList = new LinkedBlockingQueue<>(numConnections);
    }


    /*
     ** This is used for setting up a ConnectionState for the client side to perform reads
     */
    public T allocConnectionState(final AsynchronousSocketChannel chan) {

        ConnectionState conn = null;

        synchronized (queueMutex) {
            if (freeConnCount > 0) {
                freeConnCount--;
                try {
                    conn = (ConnectionState) connPoolFreeList.take();
                    conn.setChannel(chan);

                } catch (InterruptedException int_ex) {
                    System.out.println(int_ex.getMessage());
                    freeConnCount++;
                }
            }
        }

        return (T) conn;
    }

    public void freeConnectionState(T connectionState) {
        try {
            connPoolFreeList.put(connectionState);
            freeConnCount++;
        } catch (InterruptedException int_ex) {
            System.out.println(int_ex.getMessage());
        }
    }


    public int getConnId() {
        return connId;
    }

    static class QueueMutex {
        int count;
    }

}
