package com.oracle.athena.webserver.connectionstate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.channels.AsynchronousSocketChannel;

/**
 * A collection of tests that validate the {@link ConnectionStatePool} funcionality.
 */
class ConnectionStatePoolTests {

    /**
     * This test is meant to validate that, when constructed, a {@link ConnectionState} can be allocated from the pool
     */
    @Test
    void verifyAllocReturnsValue() {
        // construct the connectionStatePool
        final ConnectionStatePool<WebServerConnState> connectionStatePool = new ConnectionStatePool<>(1);
        // then populate it with a (mocked) WebServerConnState.
        connectionStatePool.freeConnectionState(Mockito.mock(WebServerConnState.class));
        // finally test that our conditions are met
        Assertions.assertNotNull(connectionStatePool.allocConnectionState(Mockito.mock(AsynchronousSocketChannel.class)));
    }

    /**
     * TODO CA: Is this desired behavior?
     * Verify that when there is no provided {@link AsynchronousSocketChannel} the
     * {@link ConnectionStatePool#allocConnectionState(AsynchronousSocketChannel)} still returns an object.
     */
    @Test
    void verifyAllocReturnsValueWithNullChannel() {
        final ConnectionStatePool<WebServerConnState> connectionStatePool = new ConnectionStatePool<>(1);
        connectionStatePool.freeConnectionState(Mockito.mock(WebServerConnState.class));
        Assertions.assertNotNull(connectionStatePool.allocConnectionState(null));
    }

    /**
     * Verify that adding a null {@link ConnectionState} to the connectionStatePool is handled gracefully. The pool
     * should behave as if no {@link ConnectionState} has been added, which means the call to
     * {@link ConnectionStatePool#allocConnectionState(AsynchronousSocketChannel)} should return null.
     */
    @Test
    void verifyAllocTreatsNullConnectionStatesAsNoOps() {
        final ConnectionStatePool<WebServerConnState> connectionStatePool = new ConnectionStatePool<>(1);
        connectionStatePool.freeConnectionState(null);
        Assertions.assertNull(connectionStatePool.allocConnectionState(Mockito.mock(AsynchronousSocketChannel.class)));
    }

    /**
     * This test is meant to validate that when a {@link ConnectionStatePool} has no extra {@link ConnectionState}
     * objects, it returns immediately with a null value.
     */
    @Test
    void verifyAllocReturnsNullWhenAtCapacity() {
        final ConnectionStatePool<WebServerConnState> connectionStatePool = new ConnectionStatePool<>(1);
        connectionStatePool.freeConnectionState(Mockito.mock(WebServerConnState.class));
        // consume the one and only connection
        final ConnectionState connectionState =
                connectionStatePool.allocConnectionState(Mockito.mock(AsynchronousSocketChannel.class));
        // then try to get something from the pool
        Assertions.assertNull(connectionStatePool.allocConnectionState(Mockito.mock(AsynchronousSocketChannel.class)));
    }

    /**
     * Verify that when a resource is consumed and then added back to the {@link ConnectionStatePool} that the resource
     * actually becomes available for reuse.
     */
    @Test
    void verifyResourceReuse() {
        final ConnectionStatePool<WebServerConnState> connectionStatePool = new ConnectionStatePool<>(1);
        connectionStatePool.freeConnectionState(Mockito.mock(WebServerConnState.class));
        // consume the one and only connection
        final WebServerConnState connectionState =
                connectionStatePool.allocConnectionState(Mockito.mock(AsynchronousSocketChannel.class));
        connectionStatePool.freeConnectionState(connectionState);
        Assertions.assertNotNull(connectionStatePool.allocConnectionState(Mockito.mock(AsynchronousSocketChannel.class)));
    }
}
