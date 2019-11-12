package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public enum Tier {

    Expedited("Expedited"),

    Standard("Standard"),

    Bulk("Bulk");

    private static final ImmutableMap<String, Tier> STRING_TO_TYPE_MAP;

    static {
        STRING_TO_TYPE_MAP = ImmutableMap.<String, Tier>builder()
                .put(Expedited.getValue(), Expedited)
                .put(Standard.getValue(), Standard)
                .put(Bulk.getValue(), Bulk)
                .build();
    }

    /**
     * The value that represents the type which may be persisted - do not change the mapping from type to value.
     */
    private final String value;

    Tier(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    /**
     * Converts to enum value. Raw string value must match one of the enum.
     */
    public static Tier fromValue(String value) {
        Preconditions.checkArgument(STRING_TO_TYPE_MAP.containsKey(value));
        return STRING_TO_TYPE_MAP.get(value);
    }
}
