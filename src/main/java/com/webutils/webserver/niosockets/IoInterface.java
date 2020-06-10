package com.webutils.webserver.niosockets;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.operations.Operation;

import java.net.InetAddress;
import java.nio.channels.SocketChannel;

/*
** This abstracts away the interface type. Currently, there are two I/O methods supported, one that
**   used the Java NIO layer to read and write to SocketChannel(s). The other uses file I/O.
 */
public interface IoInterface {

    /*
    ** There are two different startClient() interfaces, one for SocketChannel connections and another
    **   for File connections
     */
    void startClient(final SocketChannel socket);
    void startClient(final String readFileName, final Operation errorHandler);

    void registerClientErrorHandler(final Operation clientErroHandler);

    /*
    ** There are two different startInitiator() interfaces, one for SocketChannel connections and another for
    **   opening a file to write to.
     */
    boolean startInitiator(final InetAddress targetAddress, final int targetPort, final Operation connectComplete,
                           final Operation errorHandler);
    boolean startInitiator(final String writeFileName, final Operation errorHandler);


    /*
    ** There are two BufferManager(s) and their associated BufferManagerPointer(s) used to either
    **    read data into or write data out of.
     */
    void registerReadBufferManager(final BufferManager readBufferManager, final BufferManagerPointer readPtr);
    void registerWriteBufferManager(final BufferManager writeBufferManager, final BufferManagerPointer writePtr);

    void unregisterReadBufferManager();
    void unregisterWriteBufferManager();

    void readBufferAvailable();
    int performRead();

    void writeBufferReady();
    int performWrite();

    void sendErrorEvent();

    boolean updateInterestOps();
    void removeKey();
    void closeConnection();
    void connectComplete();

    String getIdentifierInfo();

    int getId();
}
