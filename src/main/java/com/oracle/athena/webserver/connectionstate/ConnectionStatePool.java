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
public class ConnectionStatePool {

    private int allocatedConnectionStates;

    private ConnectionState[] connectionStatePool;


    private int connId;

    /*
     ** The following are used to allow the LinkedBlockingQueue to not actually block
     **   by returning null when the queue is empty instead of waiting for an
     **   element to become available.
     */
    private QueueMutex queueMutex;
    private int freeConnCount;
    private LinkedBlockingQueue<ConnectionState> connPoolFreeList;


    public ConnectionStatePool(final int numConnections, final int serverBaseId) {

        allocatedConnectionStates = numConnections;
        freeConnCount = 0;

        queueMutex = new QueueMutex();

        connectionStatePool = new ConnectionState[allocatedConnectionStates];
        connPoolFreeList = new LinkedBlockingQueue<>(allocatedConnectionStates);
        for (int i = 0; i < allocatedConnectionStates; i++) {
            connectionStatePool[i] = new ConnectionState(this, (serverBaseId + i + 1));

            connectionStatePool[i].start();

            try {
                connPoolFreeList.put(connectionStatePool[i]);
                freeConnCount++;
            } catch (InterruptedException int_ex) {
                System.out.println(int_ex.getMessage());
            }
        }
    }

    public ConnectionState allocConnectionState(final AsynchronousSocketChannel chan) {
        ConnectionState conn = null;

        synchronized (queueMutex) {
            if (freeConnCount > 0) {
                freeConnCount--;
                try {
                    conn = connPoolFreeList.take();
                    conn.setChannelAndWrite(chan);
                } catch (InterruptedException int_ex) {
                    System.out.println(int_ex.getMessage());
                    conn = null;
                    freeConnCount++;
                }
            }
        }

        return conn;
    }

    /*
     ** This is used for setting up a ConnectionState for the client side to perform reads
     */
    public ConnectionState allocConnectionState(final AsynchronousSocketChannel chan, final ClientDataReadCallback clientReadCb) {

        ConnectionState conn = null;

        synchronized (queueMutex) {
            if (freeConnCount > 0) {
                freeConnCount--;
                try {
                    conn = connPoolFreeList.take();
                    conn.setChannel(chan);
                    conn.setClientReadCallback(clientReadCb);

                } catch (InterruptedException int_ex) {
                    System.out.println(int_ex.getMessage());
                    conn = null;
                    freeConnCount++;
                }
            }
        }

        return conn;
    }

    public void freeConnectionState(ConnectionState conn) {

        conn.clearChannel();

        try {
            connPoolFreeList.put(conn);
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
