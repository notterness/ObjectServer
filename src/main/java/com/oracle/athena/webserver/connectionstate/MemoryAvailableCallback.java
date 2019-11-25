package com.oracle.athena.webserver.connectionstate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryAvailableCallback {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAvailableCallback.class);

    private ConnectionState connectionState;
    private int requestedBuffers;

    MemoryAvailableCallback(final ConnectionState conn, final int numberOfNeededBuffers) {
        connectionState = conn;

        requestedBuffers = numberOfNeededBuffers;
    }

    public void memoryAvailable() {
        LOG.info("ConnectionState[" + connectionState.getConnStateId() + "] memoryAvailable");

        connectionState.memoryBuffersAreAvailable();
    }

    public int connRequestedBuffers() {
        return requestedBuffers;
    }

}
