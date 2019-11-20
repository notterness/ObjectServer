package com.oracle.athena.webserver.connectionstate;

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

    /*
    ** reservedCount is the number of connections to keep in reserve to handle low resource conditions
    **   and still be able to return an error back to the client
     */
    private int reservedCount;

    private LinkedBlockingQueue<T> connPoolFreeList;

    /*
     ** The following are used to allow the LinkedBlockingQueue to not actually block
     **   by returning null when the queue is empty instead of waiting for an
     **   element to become available.
     */
    private final Object queueMutex;
    private int freeConnCount;


    public ConnectionStatePool(final int numConnections, final int reserved) {

        allocatedConnectionStates = numConnections;
        reservedCount = reserved;
        freeConnCount = 0;

        queueMutex = new Object();

        connPoolFreeList = new LinkedBlockingQueue<>(numConnections + reservedCount);
    }


    /*
     ** This is used for setting up a ConnectionState for the client side to perform reads. This will not block
     **   if connections are not available.
     ** If no connections are available it will return null.
     */
    public T allocConnectionState(final AsynchronousSocketChannel chan) {

        ConnectionState conn = null;

        synchronized (queueMutex) {
            if (freeConnCount > reservedCount) {
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

    /*
     ** This is used for setting up a ConnectionState for the client side to perform reads. This will block
     **   if connections are not available. It will still return null if there is an exception.
     */
    public T allocConnectionStateBlocking(final AsynchronousSocketChannel chan) {

        ConnectionState conn = null;

        synchronized (queueMutex) {
            freeConnCount--;
            try {
                conn = (ConnectionState) connPoolFreeList.take();
                conn.setChannel(chan);
            } catch (InterruptedException int_ex) {
                System.out.println(int_ex.getMessage());
                freeConnCount++;
            }
        }

        return (T) conn;
    }


    public void freeConnectionState(T connectionState) {
        synchronized (queueMutex) {
            try {
                connPoolFreeList.put(connectionState);
                freeConnCount++;
            } catch (InterruptedException int_ex) {
                System.out.println(int_ex.getMessage());
            }
        }
    }

}
