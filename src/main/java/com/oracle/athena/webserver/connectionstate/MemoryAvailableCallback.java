package com.oracle.athena.webserver.connectionstate;

public class MemoryAvailableCallback {

    private ConnectionState connectionState;
    private int requestedBuffers;

    MemoryAvailableCallback(final ConnectionState conn, final int numberOfNeededBuffers) {
        connectionState = conn;

        requestedBuffers = numberOfNeededBuffers;
    }

    public void memoryAvailable() {
        System.out.println("ConnectionState[" + connectionState.getConnStateId() + "] memoryAvailable");

        connectionState.memoryBuffersAreAvailable();
    }

    public int connRequestedBuffers() {
        return requestedBuffers;
    }

}
