package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

/**
 * Response for merging/ overwriting user metadata for the object
 */
public class CopyObjectResult {

    private final Date lastModified;

    @JsonProperty("ETag")
    private final String eTag;

    public CopyObjectResult(String eTag, Date timeModifiedParam) {
        this.lastModified = new Date(timeModifiedParam.getTime());
        this.eTag = eTag;
    }

    private Date getLastModified() {
        return lastModified;
    }

    private String getETag() {
        return eTag;
    }
}

