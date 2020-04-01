package com.webutils.webserver.niosockets;

import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.requestcontext.RequestContext;

import java.nio.channels.SocketChannel;

public interface EventPollThread {

    /*
     ** Setup the Thread to handle the event loops for the SocketChannel
     */
    void start();

    /*
    ** Shutdown the thread that runs the event loops. There are two loops that run within this thread,
    **   one to perform the I/O operations and the other to run the business logic.
     */
    void stop();

    int getEventPollThreadBaseId();

    /*
    ** This allocates an IoInterface object that is used to track an individual connection.
     */
    IoInterface allocateConnection(final Operation waitingOperation);

    void releaseConnection(final IoInterface connection);

    boolean registerClientSocket(final SocketChannel clientChannel);

    boolean runComputeWork(final Operation computeOperation);

    void removeComputeWork(final Operation computeOperation);
}
