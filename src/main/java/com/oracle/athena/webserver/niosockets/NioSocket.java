package com.oracle.athena.webserver.niosockets;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.operations.Operation;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/*
** This class is responsible for handling the socket connection
 */
public class NioSocket implements IoInterface {

    /*
    ** This connection being managed by this object is associated at startup with a particular thread
    **   which in turn means there is a Selector() loop that this must use. The Selector() loop
    **   is controlled via the NioSelectHandler object.
     */
    private final NioSelectHandler nioSelectHandler;

    /*
    ** This is the connection being managed
     */
    private SocketChannel socketChannel;

    /*
    ** This is the Operation to call the event() handler on if there is an error either setting up
    **   or while using the socket (i.e. the other side disconnected).
     */
    private Operation socketErrorHandler;

    public NioSocket(final NioSelectHandler nioSelectHandler) {
        this.nioSelectHandler = nioSelectHandler;
    }

    /*
    ** This is the actual method to call to start all of the processing threads for a TCP port. The SocketChannel
    **   is assigned as part of the startClient() method to allow the NioSocket objects to be allocated out of a
    **   pool if so desired.
     */
    public void startClient(final SocketChannel socket, final Operation errorHandler) {
        socketChannel = socket;

        socketErrorHandler = errorHandler;
    }

    public void startClient(final String readFileName, final Operation errorHandler) {
        /*
        ** This is not used for the NIO based I/O
         */
    }

    /*
    ** The startInitiator() call is used to open up a connection to (at least initially) write data out of. This
    **   requires opening a connection and attaching it to a remote listener.
     */
    public boolean startInitiator(final InetAddress targetAddress, final int targetPort, final Operation errorHandler) {

        boolean success = true;

        socketErrorHandler = errorHandler;

        InetSocketAddress socketAddress = new InetSocketAddress(targetAddress, targetPort);

        try {
            socketChannel = SocketChannel.open();
        } catch (IOException io_ex) {
            /*
            ** What to do if the socket cannot be opened
             */
            return false;
        }

        /*
        ** This is in a separate try{} so that the socketChannel can be closed it if
        **   fails.
         */
        try {
            socketChannel.configureBlocking(false);

            socketChannel.connect(socketAddress);
        } catch (IOException io_ex) {
            try {
                socketChannel.close();
            } catch (IOException ex) {
                /*
                ** Unable to close the socket as it might have already been closed
                 */
            }
            socketChannel = null;

            return false;
        }

        /*
        ** Register with the selector for this thread to know when the connection is available to
        **   perform writes and reads.
         */
        if (!nioSelectHandler.registerWithSelector(socketChannel, SelectionKey.OP_CONNECT, this)) {
            try {
                socketChannel.close();
            } catch (IOException ex) {
                /*
                 ** Unable to close the socket as it might have already been closed
                 */
            }
            socketChannel = null;
            success = false;
        }

        return success;
    }

    /*
    ** The following startInitiator() is not used for the NIO I/O
     */
    public boolean startInitiator(final String writeFileName, final Operation errorHandler) {
        /*
        ** Not used for NIO
         */
        return true;
    }

    /*
    ** The following is used to register with the NIO handling layer for reads. When a server connection is made, this
    **   registration is used to know where to pass the information from the socket.
     */
    public void registerReadBufferManager(final BufferManager readBufferMgr, final BufferManagerPointer readPointer) {

    }

    public void registerWriteBufferManager(final BufferManager writeBufferManager, final BufferManagerPointer writePtr) {

    }

    public void unregisterReadBufferManager() {

    }

    public void unregisterWriteBufferManager() {

    }

    /*
     ** This is called when there is a buffer in the BufferManager that is ready to accept data from
     **   the SocketChannel
     */
    public void readBufferAvailable() {

    }

    /*
    ** This is called when there is a buffer in the BufferManager with data that is ready to be written out
    **   the SocketChannel
     */
    public void writeBufferReady() {

    }


    /*
    ** The following is used to force the NIO socket to be closed and release the resources associated with that
    **   socket.
     */
    public void closeConnection() {

    }

    /*
    ** Accessor method to call the Operation that is setup to handle when there is an error on
    **   the SocketChannel.
     */
    public void sendErrorEvent() {
        socketErrorHandler.event();
    }
}

