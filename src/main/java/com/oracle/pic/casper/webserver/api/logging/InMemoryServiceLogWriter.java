package com.oracle.pic.casper.webserver.api.logging;

import com.google.common.collect.Lists;
import com.oracle.pic.casper.common.metering.Meter;
import com.oracle.pic.casper.webserver.api.model.logging.ServiceLogEntry;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * In-Memory implementation of a {@link ServiceLogWriterImpl}
 */
public class InMemoryServiceLogWriter implements ServiceLogWriter {
    private final LinkedBlockingQueue<ServiceLogEntry> serviceLogEntries = new LinkedBlockingQueue<>();

    @Override
    public void log(ServiceLogEntry entry, boolean isApolloEnabled) {
        serviceLogEntries.add(entry);
    }

    /**
     * Return a defensive copy of the in-memory store of {@link Meter}
     */
    public List<ServiceLogEntry> getServiceLogEntries() {
        return Lists.newArrayList(serviceLogEntries);
    }

    public synchronized void clear() {
        serviceLogEntries.clear();
    }
}
