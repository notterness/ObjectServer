package com.oracle.pic.casper.webserver.api.logging;

import com.google.inject.ImplementedBy;
import com.oracle.pic.casper.webserver.api.model.logging.ServiceLogEntry;

@ImplementedBy(ServiceLogWriterImpl.class)
public interface ServiceLogWriter {

    void log(ServiceLogEntry entry,  boolean isApolloEnabled);
}
