package com.oracle.athena.webserver.client;

import com.oracle.athena.webserver.connectionstate.ConnectionState;
import com.oracle.athena.webserver.server.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


// A client connection is a single server port that can talk to multiple clients. The clients
// can open connections to this server or this TestClient can be used to send
// data to a client. The TestClient consists of a single stream of data back and forth
// between a client and a server.
// The TestConnection sits on top of the WriteConnection

public class TestClient implements Runnable {

    private static final int WORK_QUEUE_SIZE = 10;

    private int serverTargetId;
    private long nextTransactionId;

    private final Object clientConnListLock;

    private final List<WriteConnection> clientConnections;
    private InitiatorServer targetServer;

    private BlockingQueue<WriteConnection> workQueue;

    private Thread clientWriteThread;

    private final AtomicBoolean threadExit;

    private int tcpPort;

    public TestClient(final int srcClientId) {
        serverTargetId = srcClientId;
        nextTransactionId = 1;
        clientConnections = Collections.synchronizedList(new ArrayList<>());
        targetServer = null;
        threadExit = new AtomicBoolean(false);
        clientConnListLock = new Object();
    }

    public void start() {
        workQueue = new LinkedBlockingQueue<>(WORK_QUEUE_SIZE);

        clientWriteThread = new Thread(this);
        clientWriteThread.start();
    }

    public void stop() {
        threadExit.set(true);
    }


    // Each new target opens up a connection to a remote server
    public WriteConnection addNewTarget(int clientTargetId) {
        tcpPort = ServerChannelLayer.BASE_TCP_PORT + clientTargetId;
        if (targetServer == null) {
            // Setup the server for this clientId
            int serverTcpPort = ServerChannelLayer.BASE_TCP_PORT + serverTargetId;
            targetServer = new InitiatorServer(2, serverTcpPort, serverTargetId);
            targetServer.start();
        }

        long connTransactionId = getTransactionId();
        WriteConnection client = new WriteConnection(connTransactionId);
        // clientConnections is a synchronized collection so adds are thread safe
        clientConnections.add(client);
        return client;
    }

    // This returns the transaction ID for the for this target connection
    // It will open up the connection and return when the connection succeeds or it
    // times out trying.
    public boolean connectTarget(final WriteConnection writeConn, final int timeoutMs) {

        boolean connected = false;
        if (writeConn != null) {
            writeConn.open(tcpPort, timeoutMs);

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
        writeConn.close();
    }

    /*
     ** This is how the client registers to receive data read callback.
     */
    public void registerClientReadCallback(WriteConnection writeConnection, ClientDataReadCallback readCallback) {
        InitiatorLoadBalancer serverWorkHandler;
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
        System.out.println("TestClient thread() start");
        try {
            WriteConnection writeConn;
            while (!threadExit.get()) {
                if ((writeConn = workQueue.poll(10000, TimeUnit.MILLISECONDS)) != null) {
                    // Perform write to this socket
                    writeConn.writeAvailableData();
                } else {
                    // Check when last heartbeat was sent
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("TestClient thread() exit");
    }


    private synchronized long getTransactionId() {
        long transaction = nextTransactionId;

        nextTransactionId++;

        return transaction;
    }

    public boolean writeData(WriteConnection writeConn, WriteCompletion completion) {
        if (writeConn.writeData(completion)) {
            return workQueue.add(writeConn);
        }
        return false;
    }

    private WriteConnection findWriteConnection(long transactionId) {
        boolean found = false;
        WriteConnection clientConn = null;

        synchronized (clientConnListLock) {
            for (WriteConnection clientConnection : clientConnections) {
                clientConn = clientConnection;

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
}
