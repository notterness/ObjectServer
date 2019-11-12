package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.common.error.ErrorCode;

@JacksonXmlRootElement(localName = "Error")
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class S3Error {
    private final ErrorCode errorCode;
    private final String message;
    private String requestId = null;
    private String hostId = null;

    public S3Error(ErrorCode errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public S3Error(ErrorCode errorCode, String message, String requestId, String hostId) {
        this.errorCode = errorCode;
        this.message = message;
        this.requestId = requestId;
        this.hostId = hostId;
    }

    @JsonIgnore
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public String getCode() {
        return errorCode.getErrorName();
    }

    public String getMessage() {
        return message;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getHostId() {
        return hostId;
    }
}
