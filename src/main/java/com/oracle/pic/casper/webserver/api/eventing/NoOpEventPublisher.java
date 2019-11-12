package com.oracle.pic.casper.webserver.api.eventing;

import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;

/**
 * A no op event publisher that is used when the region does not have a event ingestion endpoint.
 * Used mostly for local testing or when bootstrapping a region before event ingestion is available
 */
public class NoOpEventPublisher implements EventPublisher {

    public void addEvent(CasperEvent event, String tenantId, ObjectLevelAuditMode objectLevelAuditMode) {
    }

    public void start() {
    }

    public void stopRunning() {
    }

    public void join(long millis) {
    }
}
