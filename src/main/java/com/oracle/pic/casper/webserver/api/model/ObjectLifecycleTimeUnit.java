package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ObjectLifecycleTimeUnit {

    // Internal test Time Unit.
    Milliseconds("MILLISECONDS"),
    Seconds("SECONDS"),
    Minutes("MINUTES"),

    // Public Time Unit.
    Days("DAYS"),
    Years("YEARS");

    /**
     * The value that represents the type which may be persisted - do not change the mapping from type to value.
     */
    private final String value;

    ObjectLifecycleTimeUnit(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
