package com.oracle.pic.casper.webserver.api.backend.putobject.storageserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

public final class ErrorResponse {
    @JsonProperty("status_code")
    private final String statusCode;

    @JsonProperty("code")
    private final String code;

    @JsonProperty("throwable")
    private final String throwable;

    @JsonProperty("stacktrace")
    private final String stacktrace;

    @JsonProperty("message")
    private final String message;

    @JsonCreator
    public ErrorResponse(@JsonProperty("status_code") String statusCode,
                         @JsonProperty("code") String code,
                         @JsonProperty("throwable") String throwable,
                         @JsonProperty("stacktrace") String stacktrace,
                         @JsonProperty("message") String message) {
        this.statusCode = statusCode;
        this.code = code;
        this.throwable = throwable;
        this.stacktrace = stacktrace;
        this.message = message;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public String getCode() {
        return code;
    }

    public String getThrowable() {
        return throwable;
    }

    public String getStacktrace() {
        return stacktrace;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("statusCode", statusCode)
                .add("code", code)
                .add("throwable", throwable)
                .add("stacktrace", stacktrace)
                .add("message", message)
                .toString();
    }
}
