package com.oracle.pic.casper.webserver.api.model;

/**
 * A list of all top level keys which can be expected to be present in
 * {@link com.oracle.bmc.objectstorage.model.BucketOptionsDetails}
 */
public enum BucketOptions {

    READ_LOG_ID("readLogId"),

    WRITE_LOG_ID("writeLogId"),

    EVENTS_TOGGLE("objectEventsEnabled");

    private final String value;

    BucketOptions(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
