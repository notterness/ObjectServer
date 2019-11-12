package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.oracle.pic.casper.objectmeta.ETagType;

import java.util.Objects;

/**
 * Contains details for the request to commit the upload for a multipart large object.
 */
public class FinishUploadRequest {

    private final UploadIdentifier uploadIdentifier;
    private final ETagType eTagType;
    private final ImmutableSortedSet<PartAndETag> partsToCommit;
    private final ImmutableSortedSet<Integer> partsToExclude;
    private final String ifMatch;
    private final String ifNoneMatch;

    public FinishUploadRequest(
        UploadIdentifier uploadIdentifier,
        ETagType eTagType,
        ImmutableSortedSet<PartAndETag> partsToCommit,
        ImmutableSortedSet<Integer> partsToExclude,
        String ifMatch, String ifNoneMatch) {
        this.uploadIdentifier = uploadIdentifier;
        this.eTagType = eTagType;
        this.partsToCommit = partsToCommit;
        this.partsToExclude = partsToExclude;
        this.ifMatch = ifMatch;
        this.ifNoneMatch = ifNoneMatch;
    }

    @JsonDeserialize(builder = PartAndETag.Builder.class)
    public static final class PartAndETag implements Comparable<PartAndETag> {
        @JsonProperty("partNum")
        private final int uploadPartNum;
        @JsonProperty("etag")
        private final String eTag;

        @Override
        public int compareTo(PartAndETag o) {
            int compare = Integer.compare(this.uploadPartNum, o.uploadPartNum);
            if (compare != 0) {
                return compare;
            } else {
                return this.eTag.compareTo(o.eTag);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartAndETag that = (PartAndETag) o;
            return uploadPartNum == that.uploadPartNum &&
                Objects.equals(eTag, that.eTag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uploadPartNum, eTag);
        }

        @JsonPOJOBuilder(withPrefix = "")
        public static final class Builder {
            @JsonProperty("partNum")
            private Integer uploadPartNum;
            @JsonProperty("etag")
            private String eTag;

            public Builder uploadPartNum(int val) {
                uploadPartNum = val;
                return this;
            }

            public Builder eTag(String val) {
                eTag = val;
                return this;
            }

            public PartAndETag build() {
                Preconditions.checkNotNull(uploadPartNum);
                Preconditions.checkNotNull(eTag);
                return new PartAndETag(this);
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        private PartAndETag(Builder builder) {
            uploadPartNum = builder.uploadPartNum;
            eTag = builder.eTag;
        }

        public int getUploadPartNum() {
            return uploadPartNum;
        }

        public String getEtag() {
            return eTag;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("uploadPartNum", uploadPartNum)
                    .add("eTag", eTag)
                    .toString();
        }
    }

    public UploadIdentifier getUploadIdentifier() {
        return uploadIdentifier;
    }

    public ETagType getETagType() {
        return eTagType;
    }

    public ImmutableSortedSet<PartAndETag> getPartsToCommit() {
        return partsToCommit;
    }

    public ImmutableSortedSet<Integer> getPartsToExclude() {
        return partsToExclude;
    }

    public String getIfMatch() {
        return ifMatch;
    }

    public String getIfNoneMatch() {
        return ifNoneMatch;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("uploadIdentifier", uploadIdentifier)
            .add("eTagType", eTagType)
            .add("partsToCommit", partsToCommit)
            .add("partsToExclude", partsToExclude)
            .add("ifMatch", ifMatch)
            .add("ifNoneMatch", ifNoneMatch)
            .toString();
    }
}
