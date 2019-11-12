package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.Objects;

@JsonDeserialize(builder = ReplicationSource.Builder.class)
public final class ReplicationSource {

    @JsonProperty("policyName")
    private final String policyName;

    @JsonProperty("sourceRegionName")
    private final String sourceRegionName;

    @JsonProperty("sourceBucketName")
    private final String sourceBucketName;

    public static final class Builder {
        @JsonProperty("policyName")
        private String policyName;

        @JsonProperty("sourceRegionName")
        private String sourceRegionName;

        @JsonProperty("sourceBucketName")
        private String sourceBucketName;

        public Builder policyName(String val) {
            this.policyName = val;
            return this;
        }

        public Builder sourceRegionName(String val) {
            this.sourceRegionName = val;
            return this;
        }

        public Builder sourceBucketName(String val) {
            this.sourceBucketName = val;
            return this;
        }

        public ReplicationSource build() {
            Preconditions.checkNotNull(policyName);
            Preconditions.checkNotNull(sourceRegionName);
            Preconditions.checkNotNull(sourceBucketName);

            return new ReplicationSource(this);
        }
    }

    private ReplicationSource(Builder builder) {
        this.policyName = builder.policyName;
        this.sourceRegionName = builder.sourceRegionName;
        this.sourceBucketName = builder.sourceBucketName;
    }

    public static Builder builder() {
        return new Builder();
    }


    public String getPolicyName() {
        return policyName;
    }

    public String getSourceRegionName() {
        return sourceRegionName;
    }

    public String getSourceBucketName() {
        return sourceBucketName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReplicationSource)) {
            return false;
        }
        ReplicationSource replicationSource = (ReplicationSource) o;
        return Objects.equals(policyName, replicationSource.policyName) &&
                Objects.equals(sourceRegionName, replicationSource.sourceRegionName) &&
                Objects.equals(sourceBucketName, replicationSource.sourceBucketName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policyName, sourceRegionName, sourceBucketName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("policyName", policyName)
                .add("sourceRegionName", sourceRegionName)
                .add("sourceBucketName", sourceBucketName)
                .toString();
    }
}
