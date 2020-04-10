package com.webutils.webserver.niosockets;

import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.RequestContextPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.*;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class NioEventPollThread implements Runnable, EventPollThread {

    private static final Logger LOG = LoggerFactory.getLogger(NioEventPollThread.class);

    private static final int ALLOCATED_NIO_SOCKET = 10;

    private final NioEventPollBalancer threadPoolOwner;

    private final int eventPollThreadBaseId;

    private final RequestContextPool requestContextPool;

    private final LinkedList<IoInterface> freeConnections;
    private final LinkedList<Operation> waitingForConnections;

    private volatile boolean threadRunning;

    private NioSelectHandler nioSelectHandler;

    /*
    ** uniqueRequestId is used to guarantee that for any client HTTP Request that comes into the Web Server, there is
    **   a unique key that can be used to track it through all of the trace statements.
     */
    private final AtomicInteger uniqueRequestId;


    public NioEventPollThread(final NioEventPollBalancer poolOwner, final int threadBaseId,
                              final RequestContextPool requestContextPool) {
        this.threadPoolOwner = poolOwner;
        this.eventPollThreadBaseId = threadBaseId;
        this.requestContextPool = requestContextPool;

        this.freeConnections = new LinkedList<>();
        this.waitingForConnections = new LinkedList<>();

        this.threadRunning = true;

        /*
        ** This is the request ID to track an HTTP Request through the logging.
         */
        uniqueRequestId = new AtomicInteger(1);
    }

    /*
    ** Setup the Thread to handle the event loops for the SocketChannel
     */
    public void start() {
        /*
        ** Setup NioSelectHandler for this thread.
         */
        nioSelectHandler = new NioSelectHandler();

        /*
        ** Now setup the Selector for the handler so it can be passed into the pre-allocated NioSocket
        **   connection control objects.
         */
        nioSelectHandler.setupSelector();

        /*
        ** Create a collection of NioSocket to handle communications with the Storage Servers
         */
        for (int i = 0; i < ALLOCATED_NIO_SOCKET; i++) {
            NioSocket connection = new NioSocket(nioSelectHandler);

            freeConnections.add(connection);
        }
        /*
         ** Register with the RequestContextPool (need to do this prior to starting the thread otherwise
         **   the call to run the outstanding RequestContext for the thread will spit out an error).
         */
        requestContextPool.setThreadAndBaseId(this, eventPollThreadBaseId);

        Thread eventPollThread = new Thread(this);
        eventPollThread.start();
    }

    /*
    ** Shutdown the Thread used to handle the event loop
     */
    public void stop() {
        /*
        ** Remove all the entries on the freeConnections list
         */
        int numFreeConnections = freeConnections.size();
        if (numFreeConnections != ALLOCATED_NIO_SOCKET) {
            System.out.println("[" + eventPollThreadBaseId + "] numFreeConnections: " + numFreeConnections + " expected ALLOCATED_NIO_SOCKET: " +
                    ALLOCATED_NIO_SOCKET);
        }
        freeConnections.clear();

        requestContextPool.stop(eventPollThreadBaseId);

        threadRunning = false;
    }

    public int getEventPollThreadBaseId() {
        return eventPollThreadBaseId;
    }

    /*
    ** TODO: Wire in the wakeup of the waitingOperation if there are no NioSocket
    **   available and add a test for this
     */
    public IoInterface allocateConnection(final Operation waitingOperation) {
        IoInterface connection =  freeConnections.poll();
        if (connection == null) {
            waitingForConnections.add(waitingOperation);
        }

        LOG.info("allocateConnection [" + eventPollThreadBaseId + "]");
        return connection;
    }

    public void releaseConnection(final IoInterface connection) {
        freeConnections.add(connection);

        Operation waitingOperation = waitingForConnections.poll();
        if (waitingOperation != null) {
            waitingOperation.event();
        }
    }

    /*
    ** This is where a RequestContext is acquired for a connection and the association between the connection and
    **   the SocketChannel is made. This is how the NIO layer is linked into the actual RequestContext and its
    **   associated BufferManagers.
    ** Add a client SocketChannel to the Selector
     */
    public boolean registerClientSocket(final SocketChannel clientChannel) {
        boolean success = true;

        /*
        ** Allocate the RequestContext that is used to track this HTTP Request for its lifetime. The RequestContext is
        **   the placeholder for the various state and generated information for the request.
         */
        RequestContext requestContext = requestContextPool.allocateContext(eventPollThreadBaseId);

        /*
        ** The IoInterface is the wrapper around the NIO SocketChannel code that allows communication over a socket
        **   with the client who generated this request.
         */
        if (requestContext != null) {
            IoInterface connection = allocateConnection(null);
            if (connection != null) {
                connection.startClient(clientChannel);

                int requestId = uniqueRequestId.getAndIncrement();
                requestContext.initializeServer(connection, requestId);
            } else {
                LOG.warn("[" + eventPollThreadBaseId + "] no free connections");
                success = false;
            }
        } else {
            LOG.warn("[" + eventPollThreadBaseId + "] no free requests");
            success = false;
        }

        return success;
    }


    /*
    ** The following is the Thread to handle the select events for the client SocketChannel
     */
    public void run() {

        LOG.info("eventThread[" + eventPollThreadBaseId + "] run()");

        while (threadRunning) {
            /*
            ** Perform the NIO SocketChannel work
             */
            nioSelectHandler.handleSelector();

            /*
            ** Now check if there is other work to be performed on the connections that does not deal with the
            **   SocketChanel read and write operations
             */
            requestContextPool.executeRequestContext(eventPollThreadBaseId);
        }

        LOG.info("eventThread[" + eventPollThreadBaseId + "] exit");

        nioSelectHandler.releaseSelector();
    }

    /*
     ** This adds work to the Compute Thread Pool to be picked up by a free compute thread. It currently goes through
     **   the NioEventPollBalancer to have access to the ComputeThreadPool (which is instantiated in the
     **   NioEventPollBalancer object).
     */
    public boolean runComputeWork(final Operation computeOperation) {
        if (threadPoolOwner != null) {
            threadPoolOwner.runComputeWork(computeOperation);
            return true;
        }

        return false;
    }

    public void removeComputeWork(final Operation computeOperation) {
        if (threadPoolOwner != null) {
            threadPoolOwner.removeComputeWork(computeOperation);
        }
    }
}
