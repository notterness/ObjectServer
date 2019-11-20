package com.oracle.athena.webserver.connectionstate;

public class ServerConnectionStatePool extends ConnectionStatePool<WebServerConnState> {
    /**
     * Primary constructor for a ConnectionStatePool.
     *
     * @param numConnections represents the total number of {@link ConnectionState} objects that can be tracked
     *                       by this pool. There must be at least one connection.
     */
    public ServerConnectionStatePool(int numConnections) {
        super(numConnections);

    }


}
