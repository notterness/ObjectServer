package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * This is the json object to parse bulk restore request body.
 */
public final class BulkRestoreRequestJson {

    private final List<String> inclusionPatterns;
    private final List<String> exclusionPatterns;
    private final Integer hours;

    @JsonCreator
    private BulkRestoreRequestJson(@JsonProperty("inclusionPatterns") List<String> inclusionPatterns,
                                   @JsonProperty("exclusionPatterns") List<String> exclusionPatterns,
                                   @JsonProperty("hours") Integer hours) {
        this.inclusionPatterns = inclusionPatterns;
        this.exclusionPatterns = exclusionPatterns;
        this.hours = hours;
    }

    public List<String> getExclusionPatterns() {
        return exclusionPatterns;
    }

    public List<String> getInclusionPatterns() {
        return inclusionPatterns;
    }

    public Integer getHours() {
        return hours;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BulkRestoreRequestJson that = (BulkRestoreRequestJson) o;
        return Objects.equals(inclusionPatterns, that.inclusionPatterns) &&
                Objects.equals(exclusionPatterns, that.exclusionPatterns) &&
                Objects.equals(hours, that.hours);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inclusionPatterns, exclusionPatterns, hours);
    }

    @Override
    public String toString() {
        return "BulkRestoreRequestJson{" +
                "inclusionPatterns=" + inclusionPatterns +
                ", exclusionPatterns=" + exclusionPatterns +
                ", hours=" + hours +
                '}';
    }
}
