package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.model.BucketMeterFlagSet;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.BucketStorageTier;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingBucketNameException;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingCompartmentIdException;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Buckets are containers for organizing and configuring storage objects.
 */
@JsonDeserialize(builder = Bucket.Builder.class)
public final class Bucket {

    @JsonProperty("namespace")
    private final String namespaceName;

    @JsonProperty("name")
    private final String bucketName;

    /**
     * OCID of the bucket
     */
    @JsonProperty("id")
    private final String id;

    @JsonProperty("compartmentId")
    private final String compartmentId;

    @JsonProperty("createdBy")
    private final String createdBy;

    @JsonProperty("timeCreated")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DateUtil.SWAGGER_DATE_TIME_PATTERN, timezone = "UTC")
    private final Instant timeCreated;

    @JsonProperty("metadata")
    private final Map<String, String> metadata;

    @JsonProperty("etag")
    private final String etag;

    @JsonProperty("publicAccessType")
    private final BucketPublicAccessType publicAccessType;

    @JsonProperty("storageTier")
    private final BucketStorageTier storageTier;

    @JsonProperty("meterFlagSet")
    private final BucketMeterFlagSet meterFlagSet;

    // bucket options are accessible only via the GetBucketOptions API (internal only) and
    // should not be exposed via any of the other APIs.
    @JsonIgnore
    private final String serializedOptions;

    @JsonProperty("objectLevelAuditMode")
    private final ObjectLevelAuditMode objectLevelAuditMode;

    @JsonProperty("freeformTags")
    private final Map<String, String> freeformTags;

    @JsonProperty("definedTags")
    private final Map<String, Map<String, Object>> definedTags;

    @JsonProperty("objectLifecyclePolicyEtag")
    @JsonInclude(NON_NULL)
    private final String objectLifecyclePolicyEtag;

    @JsonProperty("kmsKeyId")
    private final String kmsKeyId;

    @JsonProperty("approximateCount")
    private final Long approximateCount;

    @JsonProperty("approximateSize")
    private final Long approximateSize;

    @JsonProperty("objectEventsEnabled")
    private final boolean objectEventsEnabled;

    @JsonProperty("replicationEnabled")
    private final boolean replicationEnabled;

    @JsonProperty("isReadOnly")
    private final boolean readOnly;

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        @JsonProperty("namespace")
        private String namespaceName;
        @JsonProperty("name")
        private String bucketName;
        @JsonProperty("id")
        private String id;
        @JsonProperty("compartmentId")
        private String compartmentId;
        @JsonProperty("createdBy")
        private String createdBy;
        @JsonProperty("timeCreated")
        private Instant timeCreated;
        @JsonProperty("metadata")
        private Map<String, String> metadata;
        @JsonProperty("etag")
        private String etag;
        @JsonProperty("publicAccessType")
        private BucketPublicAccessType publicAccessType;
        @JsonProperty("storageTier")
        private BucketStorageTier storageTier;
        @JsonProperty("meterFlagSet")
        private BucketMeterFlagSet meterFlagSet;
        @JsonIgnore
        private String serializedOptions;
        @JsonProperty("objectLevelAuditMode")
        private ObjectLevelAuditMode objectLevelAuditMode;
        @JsonProperty("freeformTags")
        private Map<String, String> freeformTags;
        @JsonProperty("definedTags")
        private Map<String, Map<String, Object>> definedTags;
        @JsonProperty("objectLifecyclePolicyEtag")
        private String objectLifecyclePolicyEtag;
        @JsonProperty("kmsKeyId")
        private String kmsKeyId;
        @JsonProperty("approximateCount")
        private Long approximateCount;
        @JsonProperty("approximateSize")
        private Long approximateSize;
        @JsonProperty("objectEventsEnabled")
        private boolean objectEventsEnabled;
        @JsonProperty("replicationEnabled")
        private boolean replicationEnabled;
        @JsonProperty("isReadOnly")
        private boolean readOnly;

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

        public Builder id(String val) {
            id = val;
            return this;
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

        public Map<String, String> getMetadata() {
            if (metadata == null) {
                return ImmutableMap.of();
            }
            return metadata;
        }

        public Map<String, String> getFreeformTags() {
            if (freeformTags == null) {
                return ImmutableMap.of();
            }
            return freeformTags;
        }

        public Map<String, Map<String, Object>> getDefinedTags() {
            if (definedTags == null) {
                return ImmutableMap.of();
            }
            return definedTags;
        }

        public Builder metadata(Map<String, String> val) {
            metadata = val;
            return this;
        }

        public Builder etag(String val) {
            etag = val;
            return this;
        }

        public Builder publicAccessType(BucketPublicAccessType val) {
            publicAccessType = val;
            return this;
        }

        public Builder storageTier(BucketStorageTier val) {
            storageTier = val;
            return this;
        }

        public Builder meterFlagSet(BucketMeterFlagSet val) {
            meterFlagSet = val;
            return this;
        }

        public Builder serializedOptions(String val) {
            serializedOptions = val;
            return this;
        }

        public Builder objectLevelAuditMode(ObjectLevelAuditMode val) {
            objectLevelAuditMode = val;
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

        public Builder kmsKeyId(String val) {
            kmsKeyId = val;
            return this;
        }

        public Builder objectLifecyclePolicyEtag(String val) {
            objectLifecyclePolicyEtag = val;
            return this;
        }

        public Builder approximateCount(Long val) {
            approximateCount = val;
            return this;
        }

        public Builder approximateSize(Long val) {
            approximateSize = val;
            return this;
        }

        public Builder objectEventsEnabled(boolean val) {
            objectEventsEnabled = val;
            return this;
        }

        public Builder replicationEnabled(boolean val) {
            replicationEnabled = val;
            return this;
        }

        public Builder readOnly(boolean val) {
            readOnly = val;
            return this;
        }

        public Bucket build() {
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

            if (metadata == null) {
                metadata = ImmutableMap.of();
            }

            if (etag == null) {
                etag = UUID.randomUUID().toString();
            }

            if (publicAccessType == null) {
                publicAccessType = BucketPublicAccessType.NoPublicAccess;
            }

            if (storageTier == null) {
                storageTier = BucketStorageTier.Standard;
            }

            if (meterFlagSet == null) {
                meterFlagSet = new BucketMeterFlagSet();
            }

            if (objectLevelAuditMode == null) {
                objectLevelAuditMode = ObjectLevelAuditMode.Disabled;
            }

            if (freeformTags == null) {
                freeformTags = ImmutableMap.of();
            }

            if (definedTags == null) {
                definedTags = ImmutableMap.of();
            }

            return new Bucket(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Bucket copy) {
        Builder builder = new Builder();
        builder.namespaceName = copy.namespaceName;
        builder.bucketName = copy.bucketName;
        builder.id = copy.id;
        builder.compartmentId = copy.compartmentId;
        builder.createdBy = copy.createdBy;
        builder.timeCreated = copy.timeCreated;
        builder.metadata = copy.metadata;
        builder.etag = copy.etag;
        builder.publicAccessType = copy.publicAccessType;
        builder.storageTier = copy.storageTier;
        builder.meterFlagSet = copy.meterFlagSet.newCopy();
        builder.serializedOptions = copy.serializedOptions;
        builder.objectLevelAuditMode = copy.objectLevelAuditMode;
        builder.freeformTags = copy.freeformTags;
        builder.definedTags = copy.definedTags;
        builder.objectLifecyclePolicyEtag = copy.objectLifecyclePolicyEtag;
        builder.kmsKeyId = copy.kmsKeyId;
        builder.approximateCount = copy.approximateCount;
        builder.approximateSize = copy.approximateSize;
        builder.objectEventsEnabled = copy.objectEventsEnabled;
        builder.replicationEnabled = copy.replicationEnabled;
        builder.readOnly = copy.readOnly;
        return builder;
    }

    private Bucket(Builder builder) {
        namespaceName = builder.namespaceName;
        bucketName = builder.bucketName;
        id = builder.id;
        compartmentId = builder.compartmentId;
        createdBy = builder.createdBy;
        metadata = builder.metadata;
        timeCreated = builder.timeCreated;
        etag = builder.etag;
        publicAccessType = builder.publicAccessType;
        storageTier = builder.storageTier;
        meterFlagSet = builder.meterFlagSet;
        serializedOptions = builder.serializedOptions;
        objectLevelAuditMode = builder.objectLevelAuditMode;
        freeformTags = builder.freeformTags;
        definedTags = builder.definedTags;
        objectLifecyclePolicyEtag = builder.objectLifecyclePolicyEtag;
        kmsKeyId = builder.kmsKeyId;
        approximateCount = builder.approximateCount;
        approximateSize = builder.approximateSize;
        objectEventsEnabled = builder.objectEventsEnabled;
        replicationEnabled = builder.replicationEnabled;
        readOnly = builder.readOnly;
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

    public String getId() {
        return id;
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

    /**
     * The user-defined metadata for the bucket.
     *
     * @return the map of metadata.
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    public String getETag() {
        return etag;
    }

    public BucketPublicAccessType getPublicAccessType() {
        return publicAccessType;
    }

    public BucketStorageTier getStorageTier() {
        return storageTier;
    }

    public BucketMeterFlagSet getMeterFlagSet() {
        return meterFlagSet;
    }

    public String getSerializedOptions() {
        return serializedOptions;
    }

    public ObjectLevelAuditMode getObjectLevelAuditMode() {
        return objectLevelAuditMode;
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

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    /**
     * The number of objects in the bucket
     */
    public Long getApproximateCount() {
        return approximateCount;
    }

    /**
     * The total size of the bucket in bytes
     */
    public Long getApproximateSize() {
        return approximateSize;
    }

    public boolean isObjectEventsEnabled() {
        return objectEventsEnabled;
    }

    public boolean isReplicationEnabled() {
        return replicationEnabled;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Bucket)) {
            return false;
        }
        Bucket bucket = (Bucket) o;
        return Objects.equals(namespaceName, bucket.namespaceName) &&
                Objects.equals(bucketName, bucket.bucketName) &&
                Objects.equals(id, bucket.id) &&
                Objects.equals(compartmentId, bucket.compartmentId) &&
                Objects.equals(createdBy, bucket.createdBy) &&
                Objects.equals(timeCreated, bucket.timeCreated) &&
                Objects.equals(metadata, bucket.metadata) &&
                Objects.equals(etag, bucket.etag) &&
                Objects.equals(publicAccessType, bucket.publicAccessType) &&
                Objects.equals(storageTier, bucket.storageTier) &&
                Objects.equals(meterFlagSet, bucket.meterFlagSet) &&
                Objects.equals(serializedOptions, bucket.serializedOptions) &&
                Objects.equals(objectLevelAuditMode, bucket.objectLevelAuditMode) &&
                Objects.equals(freeformTags, bucket.freeformTags) &&
                Objects.equals(definedTags, bucket.definedTags) &&
                Objects.equals(objectLifecyclePolicyEtag, bucket.objectLifecyclePolicyEtag) &&
                Objects.equals(kmsKeyId, bucket.kmsKeyId) &&
                Objects.equals(approximateCount, bucket.approximateCount) &&
                Objects.equals(approximateSize, bucket.approximateSize) &&
                Objects.equals(objectEventsEnabled, bucket.objectEventsEnabled) &&
                Objects.equals(replicationEnabled, bucket.replicationEnabled) &&
                Objects.equals(readOnly, bucket.readOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespaceName, bucketName, id, compartmentId, createdBy, timeCreated, metadata, etag,
                publicAccessType, storageTier, meterFlagSet, serializedOptions, objectLevelAuditMode, freeformTags,
                definedTags, objectLifecyclePolicyEtag, kmsKeyId, approximateCount, approximateSize,
                objectEventsEnabled, replicationEnabled, readOnly);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("namespaceName", namespaceName)
                .add("bucketName", bucketName)
                .add("id", id)
                .add("compartmentId", compartmentId)
                .add("createdBy", createdBy)
                .add("timeCreated", timeCreated)
                .add("metadata", metadata)
                .add("etag", etag)
                .add("publicAccessType", publicAccessType)
                .add("storageTier", storageTier)
                .add("meterFlagSet", meterFlagSet)
                .add("serializedOptions", serializedOptions)
                .add("objectLevelAuditMode", objectLevelAuditMode)
                .add("freeformTags", freeformTags)
                .add("definedTags", definedTags)
                .add("objectLifecyclePolicyEtag", objectLifecyclePolicyEtag)
                .add("kmsKeyId", kmsKeyId)
                .add("approximateCount", approximateCount)
                .add("approximateSize", approximateSize)
                .add("objectEventsEnabled", objectEventsEnabled)
                .add("replicationEnabled", replicationEnabled)
                .add("readOnly", readOnly)
                .toString();
    }
}
