package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * A model object used to hold an error message and name and be returned as the body of HTTP error responses.
 *
 * The JSON representation of an error looks like:
 *
 * <code>
 *     {
 *         "code": "StableErrorCode",
 *         "message": "Human readable stuff that can change over time"
 *     }
 * </code>
 *
 * The error name is meant to identify the class of the error, and will be stable (and easy to match) for all errors.
 * The error message is meant for human readability, and may change over time.
 */
public class ErrorInfo {
    private final String code;
    private final String message;

    @JsonCreator
    public ErrorInfo(@JsonProperty("code") String code,
                     @JsonProperty("message") String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorInfo error = (ErrorInfo) o;
        return Objects.equals(getCode(), error.getCode()) &&
                Objects.equals(getMessage(), error.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCode(), getMessage());
    }

    @Override
    public String toString() {
        return "ErrorInfo{" +
                "code='" + code + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
