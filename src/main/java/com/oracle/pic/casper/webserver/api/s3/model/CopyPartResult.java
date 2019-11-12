package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;

import java.util.Date;

@JsonPropertyOrder({"LastModified", "ETag"})
public final class CopyPartResult extends S3XmlResult {
    private final Date lastModified;
    private final String eTag;

    public CopyPartResult(Date lastModified, String eTag) {
        this.lastModified = new Date(lastModified.getTime());
        this.eTag = eTag;
    }

    public Date getLastModified() {
        return new Date(lastModified.getTime());
    }

    @JsonProperty("ETag")
    public String getETag() {
        return eTag;
    }
}
