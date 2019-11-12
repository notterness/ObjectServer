package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Map;
import java.util.Optional;

/**
 * This is the JSON object to parse the request body of the UpdateBucketOptions API
 */
public final class UpdateBucketOptionsRequestJson {

    private final Map<String, Object> freeformOptions;

    @JsonCreator
    private UpdateBucketOptionsRequestJson(@JsonProperty("freeformOptions") Map<String, Object> freeformOptions) {
        this.freeformOptions = freeformOptions;
    }

    public Optional<Map<String, Object>> getOptions() {
        return Optional.ofNullable(freeformOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateBucketOptionsRequestJson that = (UpdateBucketOptionsRequestJson) o;
        return Objects.equal(freeformOptions, that.freeformOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(freeformOptions);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("freeformOptions", freeformOptions)
            .toString();
    }
}
