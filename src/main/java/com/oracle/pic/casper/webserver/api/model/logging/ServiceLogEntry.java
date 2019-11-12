package com.oracle.pic.casper.webserver.api.model.logging;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class contains the fields for logging for both Apollo and Public Logging.
 *
 * We will have two separate loggers which will log a subset of the fields to the respective logs.
 *
 * Check {@link com.oracle.pic.casper.webserver.api.logging.ServiceLogWriterImpl}
 *
 */
public final class ServiceLogEntry {

    @JsonProperty("compartment")
    private final String compartment;

    private final String resource;
    /**
     * Uncomment these when LUM is done with their changes
     */
    @JsonProperty("logObject")
    private String logObjectId;

    /**
     * While writing to log make sure that it is written as a string
     */
    @JsonProperty("content")
    private final ServiceLogContent serviceLogContent;


    public ServiceLogEntry(String compartment, String resource, ServiceLogContent serviceLogContent) {
        this.compartment = compartment;
        this.resource = resource;
        this.serviceLogContent = serviceLogContent;
    }

    public String getCompartment() {
        return compartment;
    }

    public String getResource() {
        return resource;
    }

    public ServiceLogContent getServiceLogContent() {
        return serviceLogContent;
    }

  public String getLogObjectId() {
        return logObjectId;
    }

    public void updateEndTime(long endTime) {

        this.getServiceLogContent().updateEndTime(endTime);
    }


    public void updateLogObjectId(String logObjectId) {
        this.logObjectId = logObjectId;
    }
}
