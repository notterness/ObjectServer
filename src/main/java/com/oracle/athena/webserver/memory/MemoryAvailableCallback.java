package com.oracle.athena.webserver.memory;

import com.oracle.athena.webserver.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryAvailableCallback {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAvailableCallback.class);

    private final Operation operation;
    private int requestedBuffers;

    MemoryAvailableCallback(final Operation operation, final int numberOfNeededBuffers) {
        this.operation = operation;

        requestedBuffers = numberOfNeededBuffers;
    }

    public void memoryAvailable() {
        LOG.info("ConnectionState[] memoryAvailable");

        operation.event();
    }

    public int connRequestedBuffers() {
        return requestedBuffers;
    }

}
