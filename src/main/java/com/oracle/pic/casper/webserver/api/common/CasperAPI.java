package com.oracle.pic.casper.webserver.api.common;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum that defines various APIs exposed by Casper service. This will aid in identifying which API serves the request,
 * especially when all requests land on a single Eagle port and are handled by using Virtual Host routes.
 */
public enum CasperAPI {
    V2("native"),

    V1("native-v1"),

    SWIFT("swift"),

    CONTROL("native-control"),

    S3("s3-compatible");

    private final String value;

    CasperAPI(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

}
