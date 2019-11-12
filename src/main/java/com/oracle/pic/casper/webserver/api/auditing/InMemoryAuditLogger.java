package com.oracle.pic.casper.webserver.api.auditing;


import com.oracle.pic.sherlock.common.event.AuditEventV2;

import java.util.ArrayList;
import java.util.List;

public class InMemoryAuditLogger implements AuditLogger {

    private final List<AuditEventV2> auditEntries;
    private final List<AuditEventV2> eventEntries;

    public InMemoryAuditLogger() {
        this.auditEntries = new ArrayList<>();
        this.eventEntries = new ArrayList<>();
    }

    public synchronized void log(AuditEventV2 entry) {
        if (entry.getData().getInternalDetails() != null) {
            eventEntries.add(entry);
        } else {
            auditEntries.add(entry);
        }
    }

    public synchronized List<AuditEventV2> getAuditEntries() {
        return auditEntries;
    }

    public synchronized List<AuditEventV2> getEventEntries() {
        return eventEntries;
    }

    public synchronized void clear() {
        auditEntries.clear();
        eventEntries.clear();
    }
}
