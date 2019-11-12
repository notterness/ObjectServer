package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


@JsonPropertyOrder({"ETag"})
public final class CopyPartResult {
    private final String eTag;

    public CopyPartResult(String eTag) {
        this.eTag = eTag;
    }

    @JsonProperty("ETag")
    public String getETag() {
        return eTag;
    }
}
