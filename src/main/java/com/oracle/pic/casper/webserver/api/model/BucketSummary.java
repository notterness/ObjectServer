package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingBucketNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingCompartmentIdException;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Buckets are containers for organizing and configuring storage objects. BucketSummary is the attributes of a Bucket
 * that are returned by a "list buckets" API call. At this point, that is identical to a Bucket, but without the
 * user-defined metadata. That could change in the future, as Buckets gain more configuration.
 */
@JsonDeserialize(builder = BucketSummary.Builder.class)
public final class BucketSummary {

    @JsonProperty("namespace")
    private final String namespaceName;

    @JsonProperty("name")
    private final String bucketName;

    @JsonProperty("compartmentId")
    private final String compartmentId;

    @JsonProperty("createdBy")
    private final String createdBy;

    @JsonProperty("timeCreated")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DateUtil.SWAGGER_DATE_TIME_PATTERN, timezone = "UTC")
    private final Instant timeCreated;

    @JsonProperty("etag")
    private final String etag;

    @JsonProperty("freeformTags")
    private final Map<String, String> freeformTags;

    @JsonProperty("definedTags")
    private final Map<String, Map<String, Object>> definedTags;

    @Nullable
    @JsonProperty("objectLifecyclePolicyEtag")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    /*
    * objectLifecyclePolicyEtag is set to NULL unless it's specified in bucket properties. Filter and return in the
    * response only when it'snot NULL.
     */
    private final String objectLifecyclePolicyEtag;

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        @JsonProperty("namespace")
        private String namespaceName;
        @JsonProperty("name")
        private String bucketName;
        @JsonProperty("compartmentId")
        private String compartmentId;
        @JsonProperty("createdBy")
        private String createdBy;
        @JsonProperty("timeCreated")
        private Instant timeCreated;
        @JsonProperty("etag")
        private String etag;
        @JsonProperty("freeformTags")
        private Map<String, String> freeformTags;
        @JsonProperty("definedTags")
        private Map<String, Map<String, Object>> definedTags;
        @JsonProperty("objectLifecyclePolicyEtag")
        private String objectLifecyclePolicyEtag;

        public static Builder builder() {
            return new Builder();
        }

        public Builder namespaceName(String val) {
            namespaceName = val;
            return this;
        }

        public String getNamespaceName() {
            return namespaceName;
        }

        public Builder bucketName(String val) {
            bucketName = val;
            return this;
        }

        public String getBucketName() {
            return bucketName;
        }

        public Builder compartmentId(String val) {
            compartmentId = val;
            return this;
        }

        public Builder createdBy(String val) {
            createdBy = val;
            return this;
        }

        public Builder timeCreated(Instant val) {
            timeCreated = val;
            return this;
        }

        public Builder etag(String val) {
            etag = val;
            return this;
        }

        public Builder freeformTags(Map<String, String> val) {
            freeformTags = val;
            return this;
        }

        public Builder definedTags(Map<String, Map<String, Object>> val) {
            definedTags = val;
            return this;
        }

        public Builder objectLifecyclePolicyEtag(String val) {
            objectLifecyclePolicyEtag = val;
            return this;
        }

        public BucketSummary build() {
            Preconditions.checkNotNull(namespaceName);

            if (bucketName == null) {
                throw new MissingBucketNameException();
            }

            if (compartmentId == null) {
                throw new MissingCompartmentIdException();
            }

            if (timeCreated == null) {
                timeCreated = Instant.now();
            }

            if (etag == null) {
                etag = UUID.randomUUID().toString();
            }

            return new BucketSummary(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(BucketSummary copy) {
        Builder builder = new Builder();
        builder.namespaceName = copy.namespaceName;
        builder.bucketName = copy.bucketName;
        builder.compartmentId = copy.compartmentId;
        builder.createdBy = copy.createdBy;
        builder.timeCreated = copy.timeCreated;
        builder.etag = copy.etag;
        builder.freeformTags = copy.freeformTags;
        builder.definedTags = copy.definedTags;
        builder.objectLifecyclePolicyEtag = copy.objectLifecyclePolicyEtag;
        return builder;
    }

    private BucketSummary(Builder builder) {
        namespaceName = builder.namespaceName;
        bucketName = builder.bucketName;
        compartmentId = builder.compartmentId;
        createdBy = builder.createdBy;
        timeCreated = builder.timeCreated;
        etag = builder.etag;
        freeformTags = builder.freeformTags;
        definedTags = builder.definedTags;
        objectLifecyclePolicyEtag = builder.objectLifecyclePolicyEtag;
    }

    /**
     * The name of the namespace in which the bucket resides.
     *
     * @return the name of the namespace.
     */
    public String getNamespaceName() {
        return namespaceName;
    }

    /**
     * The name of the bucket.
     *
     * @return the name of the bucket.
     */
    public String getBucketName() {
        return bucketName;
    }

    public String getCompartmentId() {
        return compartmentId;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public Instant getTimeCreated() {
        return timeCreated;
    }

    public String getETag() {
        return etag;
    }

    public Map<String, String> getFreeformTags() {
        return freeformTags;
    }

    public Map<String, Map<String, Object>> getDefinedTags() {
        return definedTags;
    }

    public String getObjectLifecyclePolicyEtag() {
        return objectLifecyclePolicyEtag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BucketSummary bucket = (BucketSummary) o;
        return Objects.equals(getNamespaceName(), bucket.getNamespaceName()) &&
                Objects.equals(getBucketName(), bucket.getBucketName()) &&
                Objects.equals(getCompartmentId(), bucket.getCompartmentId()) &&
                Objects.equals(getCreatedBy(), bucket.getCreatedBy()) &&
                Objects.equals(getTimeCreated(), bucket.getTimeCreated()) &&
                Objects.equals(etag, bucket.etag) &&
                Objects.equals(getFreeformTags(), bucket.getFreeformTags()) &&
                Objects.equals(getDefinedTags(), bucket.getDefinedTags()) &&
                Objects.equals(getObjectLifecyclePolicyEtag(), bucket.getObjectLifecyclePolicyEtag());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNamespaceName(), getBucketName(), getCompartmentId(), getCreatedBy(), getTimeCreated(),
                etag,  getFreeformTags(), getDefinedTags(), getObjectLifecyclePolicyEtag());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("namespaceName", namespaceName)
                .add("bucketName", bucketName)
                .add("compartmentId", compartmentId)
                .add("createdBy", createdBy)
                .add("timeCreated", timeCreated)
                .add("etag", etag)
                .add("freeformTags", freeformTags)
                .add("definedTags", definedTags)
                .add("objectLifecyclePolicyEtag", objectLifecyclePolicyEtag)
                .toString();
    }
}
