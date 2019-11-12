package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.common.error.S3ErrorCode;

@JacksonXmlRootElement(localName = "Error")
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class S3IncorrectRegionError extends S3Error {
    private String region;

    public S3IncorrectRegionError(String requestId, String hostId, String region, String message) {
        super(S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED, message, requestId, hostId);
        this.region = region;
    }

    public String getRegion() {
        return region;
    }
}
