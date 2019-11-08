package com.oracle.athena.webserver.server;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


// A client connection is a single server port that can talk to multiple clients. The clients
// can open connections to this server or this ClientConnection can be used to send
// data to a client. The ClientConnection consists of a single stream of data back and forth
// between a client and a server.
// The ClientConnection sits on top of the WriteConnection

public class ClientConnection implements Runnable {

    private final int WORK_QUEUE_SIZE = 10;

    private int serverTargetId;
    private long nextTransactionId;

    private ListLock clientConnListLock;

    private List<WriteConnection> clientConnections;
    private ServerChannelLayer targetServer;

    private BlockingQueue<WriteConnection> workQueue;
    private Thread clientWriteThread;
    private boolean threadExit;

    private int tcpPort;

    private MemoryManager memoryManager;

    public ClientConnection(final MemoryManager memoryManager, final int srcClientId) {
        serverTargetId = srcClientId;

        this.memoryManager = memoryManager;

        nextTransactionId = 1;

        clientConnections = new ArrayList<>();

        targetServer = null;

        threadExit = false;

        clientConnListLock = new ListLock();
    }

    public void start() {
        workQueue = new LinkedBlockingQueue<>(WORK_QUEUE_SIZE);

        clientWriteThread = new Thread(this);
        clientWriteThread.start();
    }

    public void stop() {
        System.out.println("ClientConnection stop()");
        threadExit = true;

        try {
            clientWriteThread.join(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("Unable to join client write thread: " + int_ex.getMessage());
        }
    }


    // Each new target opens up a connection to a remote server
    public WriteConnection addNewTarget(int clientTargetId) {

        boolean found = false;

        tcpPort = ServerChannelLayer.BASE_TCP_PORT + clientTargetId;
        if (targetServer == null) {
            // Setup the server for this clientId
            int serverTcpPort = ServerChannelLayer.BASE_TCP_PORT + serverTargetId;
            targetServer = new ServerChannelLayer(2, serverTcpPort, serverTargetId);
            targetServer.start();
        }

        long connTransactionId = getTransactionId();
        WriteConnection client = new WriteConnection(connTransactionId);

        synchronized (clientConnections) {
            clientConnections.add(client);
        }

        return client;
    }

    // This returns the transaction ID for the for this target connection
    // It will open up the connection and return when the connection succeeds or it
    // times out trying.
    public boolean connectTarget(final WriteConnection writeConn, final int timeoutMs) {

        boolean connected = false;
        if (writeConn != null) {
            writeConn.openWriteConnection(tcpPort, timeoutMs);

            connected = true;
        } else {
            System.out.println("No WriteConnection found");
        }

        return connected;
    }

    /*
     ** This closes out the WriteConnection and cleans up anything associated with that
     ** WriteConnection.
     */
    public void disconnectTarget(WriteConnection writeConn) {
        writeConn.closeWriteConnection();
    }

    /*
     ** This is how the client registers to receive data read callback.
     */
    public void registerClientReadCallback(WriteConnection writeConnection, ClientDataReadCallback readCallback) {

        ServerLoadBalancer serverWorkHandler;
        ConnectionState work;

        serverWorkHandler = targetServer.getLoadBalancer();

        work = serverWorkHandler.startNewClientReadConnection(writeConnection.getWriteChannel(), readCallback);
        if (work != null) {
            writeConnection.setClientConnectionState(work);
        } else {
            //TODO: How should this error condition be handled?
        }
    }

    /*
     **
     ** TODO: The single thread to handle all the writes doesn't really work since the writes
     **   per connection need to be ordered and a write needs to complete before the next
     **   one is allowed to start. Something more like a map that states a connection has
     **   work pending and then calling the writeAvailableData() might be a better solution.
     */
    public void run() {
        System.out.println("ClientConnection thread() start");
        try {
            WriteConnection writeConn;

            while (!threadExit) {
                if ((writeConn = workQueue.poll(10000, TimeUnit.MILLISECONDS)) != null) {
                    // Perform write to this socket
                    if (writeConn != null) {
                        writeConn.writeAvailableData();
                    } else {
                        System.out.println("WriteConnectionHandler no write data ");
                    }
                } else {
                    // Check when last heartbeat was sent
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("ClientConnection thread() exit");
    }


    private synchronized long getTransactionId() {
        long transaction = nextTransactionId;

        nextTransactionId++;

        return transaction;
    }

    public boolean writeData(WriteConnection writeConn, WriteCompletion completion) {
        writeConn.writeData(completion);
        workQueue.add(writeConn);

        return true;
    }

    private WriteConnection findWriteConnection(long transactionId) {
        boolean found = false;
        WriteConnection clientConn = null;

        synchronized (clientConnListLock) {
            Iterator iter = clientConnections.iterator();

            while (iter.hasNext()) {
                clientConn = (WriteConnection) iter.next();

                if (clientConn.getTransactionId() == transactionId) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                clientConn = null;
            }
        }

        return clientConn;
    }


    // Need a class to use for the list lock to protect the clientConnections List
    class ListLock {
        int lockCount;
    }
}
