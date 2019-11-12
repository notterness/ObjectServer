package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A representation of an upload paginated list with zero or more items and a flag indicating that there are more.
 */
public class UploadPaginatedList {
    /**
     * A list of zero or more uploads.
     */
    private final List<UploadMetadata> uploads;

    /**
     * A list of zero or more commonPrefixes.
     */
    private final List<String> commonPrefixes;

    /**
     * True if there are more items to be fetched, false otherwise. This is always false if items is empty.
     */
    private final boolean isTruncated;

    public UploadPaginatedList(@JsonProperty("keys") Iterable<UploadMetadata> uploads,
                               @Nullable Iterable<String> commonPrefixes,
                               boolean isTruncated) {
        this.uploads = ImmutableList.copyOf(uploads);
        this.commonPrefixes = commonPrefixes == null ? null : ImmutableList.copyOf(commonPrefixes);
        this.isTruncated = isTruncated;
    }

    public List<UploadMetadata> getUploads() {
        return uploads;
    }

    public List<String> getCommonPrefixes() {
        return commonPrefixes;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UploadPaginatedList)) return false;
        UploadPaginatedList that = (UploadPaginatedList) o;
        return isTruncated == that.isTruncated &&
                Objects.equals(uploads, that.uploads) &&
                Objects.equals(commonPrefixes, that.commonPrefixes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uploads, commonPrefixes, isTruncated);
    }

    @Override
    public String toString() {
        return "UploadPaginatedList{" +
                "uploads=" + uploads +
                ", commonPrefixes=" + commonPrefixes +
                ", isTruncated=" + isTruncated +
                '}';
    }
}
