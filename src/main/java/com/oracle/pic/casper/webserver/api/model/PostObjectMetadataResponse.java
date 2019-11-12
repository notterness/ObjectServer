package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

/**
 * Response for merging/ overwriting user metadata for the object
 */
public class PostObjectMetadataResponse {

    private final Date timeModified;

    @JsonProperty("ETag")
    private final String eTag;

    public PostObjectMetadataResponse(String eTag, Date timeModifiedParam) {
        this.timeModified = new Date(timeModifiedParam.getTime());
        this.eTag = eTag;
    }

    private Date getTimeModified() {
        return timeModified;
    }

    private String getETag() {
        return eTag;
    }
}

