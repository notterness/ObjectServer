package com.oracle.pic.casper.webserver.api.eventing;

import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;

public interface EventPublisher {
    void addEvent(CasperEvent event, String tenandId, ObjectLevelAuditMode auditMode);

    void start();

    void stopRunning();

    void join(long millis) throws InterruptedException;
}
