package com.oracle.athena.webserver.connectionstate;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages a pool of {@link ConnectionState} objects, allowing them to be allocated and deallocated without
 * being garbage collected.
 *
 * @param <T> the type of {@link ConnectionState} object that this pool is managing.
 */
public class ConnectionStatePool<T extends ConnectionState> {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStatePool.class);

    // FIXME CA: This variable will need to be used for something at some point
    private final int allocatedConnectionStates;
    final LinkedBlockingQueue<T> connPoolFreeList;

    /**
     * Primary constructor for a ConnectionStatePool.
     *
     * @param numConnections represents the total number of {@link ConnectionState} objects that can be tracked
     *                       by this pool. There must be at least one connection.
     */
    public ConnectionStatePool(final int numConnections) {
        allocatedConnectionStates = numConnections;
        connPoolFreeList = new LinkedBlockingQueue<>(numConnections);
    }

    /**
     * A non-blocking method of setting up the {@link ConnectionState} for the client to perform reads.
     *
     * @param chan
     *  an {@link AsynchronousSocketChannel} to assign to a {@link ConnectionState} from this {@link ConnectionStatePool}.
     * @return <T> the {@link ConnectionState} with the channel assigned to it, null if no connections are available.
     *
     */
    public T allocConnectionState(final AsynchronousSocketChannel chan) {
        T conn = connPoolFreeList.poll();
        if (conn != null) {
            conn.setChannel(chan);
        }
        return conn;
    }

    /**
     * This method frees up a connection by adding it back to the pool. This method will block indefinitely until the
     * connectionState can be added back to the pool.
     * <p>
     * TODO CA: Should the caller be forced to wait indefinitely?
     *
     * @param connectionState a {@link ConnectionState} object to return to the free pool. Null is a no-op.
     */
    public void freeConnectionState(T connectionState) {
        if (connectionState != null) {
            try {
                connPoolFreeList.put(connectionState);
            } catch (InterruptedException int_ex) {
                // FIXME CA: Sort out how we're going to handle this exception
                LOG.info(int_ex.getMessage());
            }
        }
    }
}
