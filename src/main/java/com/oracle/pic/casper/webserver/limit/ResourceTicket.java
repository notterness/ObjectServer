package com.oracle.pic.casper.webserver.limit;

import com.oracle.pic.casper.common.resourcecontrol.ResourceUsageRecord;

public class ResourceTicket implements AutoCloseable {

    private final ResourceLimiter resourceLimiter;
    private final ResourceType resourceType;
    private final String namespace;
    private final ResourceUsageRecord record;
    private boolean closed;

    public ResourceTicket(ResourceLimiter resourceLimiter,
                          ResourceType resourceType,
                          String namespace,
                          ResourceUsageRecord record) {
        this.resourceLimiter = resourceLimiter;
        this.resourceType = resourceType;
        this.namespace = namespace;
        this.record = record;
        this.closed = false;
    }

    /**
     * Release the resource that was acquired previously. This function is not thread safe.
     * Do not try to close the ticket from multiple threads.
     */
    @Override
    public void close() {
        if (!closed) {
            resourceLimiter.release(namespace, resourceType, record);
            closed = true;
        }
    }
}
