package com.oracle.pic.casper.webserver.api.auditing;

import com.oracle.pic.sherlock.common.event.AuditEventV2;

public interface AuditLogger {

    void log(AuditEventV2 entry);
}
