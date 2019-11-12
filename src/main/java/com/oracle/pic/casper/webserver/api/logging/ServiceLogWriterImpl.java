package com.oracle.pic.casper.webserver.api.logging;

import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.json.JsonSerializer;
import com.oracle.pic.casper.webserver.api.model.logging.ServiceLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A logging implementation of {@link ServiceLogWriter} that delegates the writing to a
 * SLF4J logger.
 */
public class ServiceLogWriterImpl implements ServiceLogWriter {
    private static final String APOLLO_LOGGER_NAME = "com.oracle.pic.casper.webserver.api.logging.service_log";
    private static final String PUBLIC_LOGGER_NAME = "com.oracle.pic.casper.webserver.api.logging.public_log";

    private final Logger apolloLogger;

    private final Logger publicLogger;

    private final JsonSerializer jsonSerializer;

    public ServiceLogWriterImpl(JsonSerializer jsonSerializer) {
        this.jsonSerializer = jsonSerializer;
        this.apolloLogger = LoggerFactory.getLogger(APOLLO_LOGGER_NAME);
        this.publicLogger = LoggerFactory.getLogger(PUBLIC_LOGGER_NAME);

    }

    @Override
    public void log(ServiceLogEntry serviceLogEntry, boolean isApolloEnabled) {
        String content = jsonSerializer.toJson(serviceLogEntry.getServiceLogContent());
        publicLogger.info(jsonSerializer.toJson(ImmutableMap.of("content", content,
                "logObject", serviceLogEntry.getLogObjectId())));
        if (isApolloEnabled) {
            apolloLogger.info(jsonSerializer.toJson(ImmutableMap.of("compartment", serviceLogEntry.getCompartment(),
                    "resource", serviceLogEntry.getResource(),
                    "content", content)));
        }
    }
}
