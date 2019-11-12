package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import org.apache.commons.lang3.BooleanUtils;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * BucketUpdate represents a partial update to an existing bucket.
 *
 * Buckets are represented as JSON objects in the Casper API, and that means that any particular property of a bucket
 * can have three states: missing, null or present. For example, consider the following three bucket representations in
 * JSON (and their "metadata" property):
 *
 * <code>
 *     {
 *         "namespace": "ns",
 *         "bucket": "b"
 *     }
 *
 *     {
 *         "namespace": "ns",
 *         "bucket": "b",
 *         "metadata": null,
 *     }
 *
 *     {
 *         "namespace": "ns",
 *         "bucket": "b",
 *         "metadata": {
 *             "a": "b",
 *             "c": null,
 *         }
 *     }
 * </code>
 *
 * The first JSON object has no metadata property, the second has a null value and the last has an object value.
 *
 * This class provides a boolean value that indicates whether a property was missing or not. The "metadata" property,
 * for example, has {@link #isMissingMetadata()} and {@link #getMetadata()}, the former of which indicates whether or
 * not the latter is meaningful. The latter may be null, to indicate an explicit null was sent for the value.
 */
public final class BucketUpdate {

    private final String namespaceName;
    private final String compartmentId;
    private final String bucketName;
    private final Map<String, String> metadata;
    private final boolean isMissingMetadata;
    private final BucketPublicAccessType bucketPublicAccessType;
    private final ObjectLevelAuditMode objectLevelAuditMode;
    private final Map<String, String> freeformTags;
    private final Map<String, Map<String, Object>> definedTags;
    private final String objectLifecyclePolicyEtag;
    private final String kmsKeyId;
    private final Boolean objectEventsEnabled;

    /**
     * Constructor
     *
     * @param namespaceName the namespace name, non-null.
     * @param compartmentId the compartmentId the bucket in.
     * @param bucketName the bucket name, non-null.
     * @param metadata the metadata map, which may be null to indicate that it was explicitly set to null in the JSON
     *                 from which it was deserialized. If isMissingMetadata is true, metadata must be null. Note that
     *                 this map may contain null values, so it cannot be an
     *                 {@link com.google.common.collect.ImmutableMap}.
     * @param isMissingMetadata true to indicate that the JSON from which this object was deserialized did not contain
     *                          a "metadata" field.
     * @param bucketPublicAccessType an optional param to specify the public access type
     * @param freeformTags freeformTags to update
     * @param definedTags definedTags to update
     * @param objectLifecyclePolicyEtag
     * @param kmsKeyId optional kmsKeyId to update for the bucket
     */
    private BucketUpdate(String namespaceName,
                         @Nullable String compartmentId,
                         String bucketName,
                         Map<String, String> metadata,
                         boolean isMissingMetadata,
                         BucketPublicAccessType bucketPublicAccessType,
                         ObjectLevelAuditMode objectLevelAuditMode,
                         Map<String, String> freeformTags,
                         Map<String, Map<String, Object>> definedTags,
                         @Nullable String objectLifecyclePolicyEtag,
                         @Nullable String kmsKeyId,
                         Boolean objectEventsEnabled) {
        Preconditions.checkArgument(!isMissingMetadata || metadata == null);
        this.namespaceName = Preconditions.checkNotNull(namespaceName);
        this.compartmentId = compartmentId;
        this.bucketName = Preconditions.checkNotNull(bucketName);
        this.metadata = metadata;
        this.isMissingMetadata = isMissingMetadata;
        this.bucketPublicAccessType = bucketPublicAccessType;
        this.objectLevelAuditMode = objectLevelAuditMode;
        this.freeformTags = freeformTags;
        this.definedTags = definedTags;
        this.objectLifecyclePolicyEtag = objectLifecyclePolicyEtag;
        this.kmsKeyId = kmsKeyId;
        this.objectEventsEnabled = objectEventsEnabled;
    }

    /**
     * Get the name of the namespace in which this bucket lives.
     * @return the namespace name, non-null.
     */
    public String getNamespaceName() {
        return namespaceName;
    }

    /**
     * Get the optional compartmentId in which this bucket lives.
     * @return the optional compartmentId
     */
    public Optional<String> getCompartmentId() {
        return Optional.ofNullable(compartmentId);
    }

    /**
     * Get the name of the bucket.
     * @return the bucket name, non-null.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * The user-defined metadata for the bucket.
     * @return the map of user defined metadata, which may be null or a map with zero or more keys and values. The
     *         values of any of the keys may be null. Always null if {@link #isMissingMetadata()} is true.
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * True if the metadata for this bucket update was not explicitly set in the JSON from which it was deserialized.
     * @return true if metadata was not set, false otherwise. If this is true, {@link #getMetadata()}} always returns
     * null.
     */
    public boolean isMissingMetadata() {
        return isMissingMetadata;
    }

    /**
     * Gets the public access type of the bucket.
     */
    public Optional<BucketPublicAccessType> getPublicAccessType() {
        return Optional.ofNullable(bucketPublicAccessType);
    }

    /**
     * Gets the object-level audit logging mode of the bucket.
     */
    public Optional<ObjectLevelAuditMode> getObjectLevelAuditMode() {
        return Optional.ofNullable(objectLevelAuditMode);
    }

    /**
     * Gets the current lifecycle policy etag of the bucket.
     */
    public Optional<String> getObjectLifecyclePolicyEtag() {
        return Optional.ofNullable(objectLifecyclePolicyEtag);
    }

    public Map<String, String> getFreeformTags() {
        return freeformTags;
    }

    public Map<String, Map<String, Object>> getDefinedTags() {
        return definedTags;
    }

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    public Boolean isObjectEventsEnabled() {
        return objectEventsEnabled;
    }

    /**
     * Returns a map containing the key/value pairs that are to be applied to the existing map of options. The
     * returned map is a delta that is applied to the existing map using the logic for merging options in
     * {@link com.oracle.pic.casper.webserver.api.common.OptionsUpdater}
     *
     * NOTE: This method should only be invoked when objectsEventEnabled != null. objectsEventEnabled == null
     * indicates that no options were specified in the bucket update request and therefore the options map should be
     * left unchanged.
     *
     * @return A map of key/value pairs to be added/updated/removed.
     */
    public Map<String, Object> getOptions() {
        // false is the default value, pass null so that the key can be removed from the existing options
        Object value = BooleanUtils.isTrue(objectEventsEnabled) ? true : null;
        return new HashMap<String, Object>() {
            {
                put("objectEventsEnabled", value);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BucketUpdate)) return false;
        BucketUpdate that = (BucketUpdate) o;
        return isMissingMetadata == that.isMissingMetadata &&
                Objects.equals(objectEventsEnabled, that.objectEventsEnabled) &&
                Objects.equals(namespaceName, that.namespaceName) &&
                Objects.equals(compartmentId, that.compartmentId) &&
                Objects.equals(bucketName, that.bucketName) &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(bucketPublicAccessType, that.bucketPublicAccessType) &&
                Objects.equals(objectLevelAuditMode, that.objectLevelAuditMode) &&
                Objects.equals(freeformTags, that.freeformTags) &&
                Objects.equals(definedTags, that.definedTags) &&
                Objects.equals(objectLifecyclePolicyEtag, that.objectLifecyclePolicyEtag) &&
                Objects.equals(kmsKeyId, that.kmsKeyId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespaceName, compartmentId, bucketName, metadata, isMissingMetadata,
                bucketPublicAccessType, objectLevelAuditMode, freeformTags, definedTags, objectLifecyclePolicyEtag,
                kmsKeyId, objectEventsEnabled);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("namespaceName", namespaceName)
                .add("compartmentId", compartmentId)
                .add("bucketName", bucketName)
                .add("metadata", metadata)
                .add("isMissingMetadata", isMissingMetadata)
                .add("bucketPublicAccessType", bucketPublicAccessType)
                .add("objectLevelAuditMode", objectLevelAuditMode)
                .add("freeformTags", freeformTags)
                .add("definedTags", definedTags)
                .add("objectLifecyclePolicyEtag", objectLifecyclePolicyEtag)
                .add("kmsKeyId", kmsKeyId)
                .add("objectEventsEnabled", objectEventsEnabled)
                .toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String namespaceName;
        private String compartmentId;
        private String bucketName;
        private Map<String, String> metadata;
        private boolean isMissingMetadata = false;
        private BucketPublicAccessType bucketPublicAccessType;
        private ObjectLevelAuditMode objectLevelAuditMode;
        private Map<String, String> freeformTags;
        private Map<String, Map<String, Object>> definedTags;
        private String objectLifecyclePolicyEtag;
        private String kmsKeyId;
        private Boolean objectEventsEnabled = null;

        public Builder namespace(String namespaceName) {
            this.namespaceName = namespaceName;
            return this;
        }

        public Builder compartmentId(String compartmentId) {
            this.compartmentId = compartmentId;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder isMissingMetadata(boolean isMissingMetadata) {
            this.isMissingMetadata = isMissingMetadata;
            return this;
        }

        public Builder bucketPublicAccessType(BucketPublicAccessType bucketPublicAccessType) {
            this.bucketPublicAccessType = bucketPublicAccessType;
            return this;
        }

        public Builder objectLevelAuditMode(ObjectLevelAuditMode objectLevelAuditMode) {
            this.objectLevelAuditMode = objectLevelAuditMode;
            return this;
        }

        public Builder freeformTags(Map<String, String> freeformTags) {
            this.freeformTags = freeformTags;
            return this;
        }

        public Builder definedTags(Map<String, Map<String, Object>> definedTags) {
            this.definedTags = definedTags;
            return this;
        }

        public Builder objectLifecyclePolicyEtag(String objectLifecyclePolicyEtag) {
            this.objectLifecyclePolicyEtag = objectLifecyclePolicyEtag;
            return this;
        }

        public Builder kmsKeyId(String kmsKeyId) {
            this.kmsKeyId = kmsKeyId;
            return this;
        }

        public Builder objectEventsEnabled(boolean objectEventsEnabled) {
            this.objectEventsEnabled = objectEventsEnabled;
            return this;
        }

        public BucketUpdate build() {
            return new BucketUpdate(namespaceName, compartmentId, bucketName, metadata, isMissingMetadata,
                    bucketPublicAccessType, objectLevelAuditMode, freeformTags, definedTags,
                    objectLifecyclePolicyEtag, kmsKeyId, objectEventsEnabled);
        }
    }
}
