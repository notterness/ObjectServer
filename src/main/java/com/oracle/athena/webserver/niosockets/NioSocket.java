package com.oracle.athena.webserver.niosockets;

/*
** This class is responsible for setting up the socket connection
 */
public class NioSocket {

    final int tcpListenPort;

    /*
    ** The threadBaseId is used to uniquely identify the thread that is performing work.
     */
    final int threadBaseId;

    /*
    ** The numberOfThreads is the number of threads used to run the poll() handling loops.
    **   The idea is that there are a limited number of sockets associated with each
    **   poll() thread to distribute the workload.
     */
    final int numPollThreads;


    NioSocket(final int tcpListenPort, final int threadBaseId, final int numThreads) {

        this.tcpListenPort = tcpListenPort;
        this.threadBaseId = threadBaseId;
        this.numPollThreads = numThreads;
    }

    /*
    ** This is the actual method to call to start all of the processing threads for a TCP port.
     */
    void start() {

    }

    /*
    ** The following is used to shutdown the threads used to handle the NIO sockets and cleanup
    **   the resources.
     */
    void stop() {

    }

    /*
    ** The following is used to register with the NIO handling layer. When a server connection is made, this
    **   registration is used to know where to pass the information from the socket.
     */
    void registerNioClient() {

    }

    /*
    ** The following is used to inform the NIO layer that there are buffers awaiting reads in the read
    **   RingBuffer
     */
    void buffersReadyForRead() {

    }

    /*
    ** The following is used to inform the NIO layer that there are buffers ready to be written out. The buffers
    **   are sitting in the NIO socket's control structure write RingBuffer
     */
    void buffersReadyToWrite() {

    }

    /*
    ** This is used to open an initiator side socket connection
     */
    void openNioConnection() {

    }

    /*
    ** The following is used to force the NIO socket to be closed and release the resources associated with that
    **   socket.
     */
    void closeNioConnection() {

    }

}

