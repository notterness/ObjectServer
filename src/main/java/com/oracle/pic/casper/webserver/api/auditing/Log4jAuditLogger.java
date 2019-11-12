package com.oracle.pic.casper.webserver.api.auditing;

import com.oracle.pic.casper.common.json.JsonSerializer;
import com.oracle.pic.sherlock.common.event.AuditEventV2;
import com.oracle.pic.sherlock.common.event.MuxedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4jAuditLogger implements AuditLogger {
    private static final Logger LOG = LoggerFactory.getLogger(Log4jAuditLogger.class);

    private final JsonSerializer jsonSerializer;
    private final Logger auditLogger;

    public Log4jAuditLogger(JsonSerializer jsonSerializer) {
        this.jsonSerializer = jsonSerializer;
        this.auditLogger = LoggerFactory.getLogger(AuditLogger.class);
    }

    @Override
    public void log(AuditEventV2 entry) {
        final String compartmentId = entry.getData().getCompartmentId();
        final String serializedEntry = jsonSerializer.toJson(entry);
        final MuxedEvent muxedEvent = new MuxedEvent(compartmentId, serializedEntry);
        final String serializedMuxedEvent = jsonSerializer.toJson(muxedEvent);
        this.auditLogger.info(serializedMuxedEvent);
    }
}
