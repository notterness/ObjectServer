package com.oracle.athena.webserver.niosockets;

import com.oracle.athena.webserver.operations.Operation;

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

    /*
    ** This allocates an IoInterface object that is used to track an individual connection.
     */
    IoInterface allocateConnection(final Operation waitingOperation);

    void releaseConnection(final IoInterface connection);

    /*
    ** This is used to tell the IoInterface that there are buffers available to read data into in the
    **   read BufferManager.
     */
    void handleRead(final IoInterface connection);

    /*
    ** This is used to tell the IoInterface that there are buffers available to read data into in the
    **   write BufferManager.
     */
    void handleWrite(final IoInterface connection);

    /*
    ** This is used to close out the connection managed by the IoInterface. It can be called when all the users
    **   of the connection are done or if an error occurs.
     */
    void closeConnection(final IoInterface connection);

}
