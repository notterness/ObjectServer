package com.oracle.athena.webserver.connectionstate;

import javax.net.ssl.SSLContext;
import java.nio.channels.AsynchronousSocketChannel;

/**
 * A {@link BlockingConnectionStatePool} is a {@link ConnectionStatePool} that enforces synchronous interactions to
 * {@link ConnectionState} objects by maintaining a thread-safe pool of them.
 *
 * @param <T> a {@link ConnectionState} or child thereof for this pool to manage.
 */
public class BlockingConnectionStatePool<T extends ConnectionState> extends ConnectionStatePool<T> {

    public BlockingConnectionStatePool(int numConnections) {
        super(numConnections);
    }


    /*
     ** This is used for setting up a ConnectionState for the client side to perform reads. This will block
     **   if connections are not available. It will still return null if there is an exception.
     */
    @Override
    public T allocConnectionState(final AsynchronousSocketChannel chan, SSLContext sslcontext) {
        T conn = null;
        try {
            conn = connPoolFreeList.take();
            conn.setChannel(chan);
        } catch (InterruptedException e) {
            // FIXME CA: We need to handle the exception in some manner - maybe it's as simple as logging it.
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public T allocConnectionState(final AsynchronousSocketChannel chan) {
        return allocConnectionState(chan, null);
    }

}
