package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.BucketStorageTier;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;

/**
 * BucketCreate represents the fields necessary to create a bucket via the API.
 *
 * Note that no validation of the bucket name, namespace name, compartment ID, created-by or metadata fields are done
 * by this class, so consumers of these fields must do their own validation.
 */
public final class BucketCreate {
    private final String namespaceName;
    private final String bucketName;
    private final String compartmentId;
    private final String createdBy;
    private final Map<String, String> metadata;
    private final BucketPublicAccessType bucketPublicAccessType;
    private final BucketStorageTier bucketStorageTier;
    private final ObjectLevelAuditMode objectLevelAuditMode;
    private final Map<String, String> freeformTags;
    private final Map<String, Map<String, Object>> definedTags;
    private final String kmsKeyId;
    private final boolean objectEventsEnabled;

    public BucketCreate(String namespaceName,
                        String bucketName,
                        String compartmentId,
                        String createdBy,
                        @Nullable Map<String, String> metadata) {
        this(namespaceName, bucketName, compartmentId, createdBy, metadata, BucketPublicAccessType.NoPublicAccess);
    }

    public BucketCreate(String namespaceName,
                        String bucketName,
                        String compartmentId,
                        String createdBy,
                        @Nullable Map<String, String> metadata,
                        BucketPublicAccessType bucketPublicAccessType) {
        this(namespaceName, bucketName, compartmentId, createdBy, metadata, bucketPublicAccessType,
                BucketStorageTier.Standard, ObjectLevelAuditMode.Disabled, null, null, null, false);
    }

    /**
     * Constructor.
     * @param namespaceName The namespace in which the bucket will be created, non-null.
     * @param bucketName The name of the bucket to create, non-null.
     * @param compartmentId The compartment that is used to auth operations on the bucket, non-null.
     * @param createdBy The OCID of the user creating the bucket, non-null.
     * @param metadata The user-defined metadata for the bucket, null to indicate that there is no metadata (internally
*                 this is stored as an empty map).
     * @param objectLevelAuditMode The object-level audit logging mode of the bucket.
     * @param freeformTags the freeformTags of a bucket
     * @param definedTags the definedTags of a bucket
     */
    public BucketCreate(String namespaceName,
                        String bucketName,
                        String compartmentId,
                        String createdBy,
                        @Nullable Map<String, String> metadata,
                        BucketPublicAccessType bucketPublicAccessType,
                        BucketStorageTier bucketStorageTier,
                        ObjectLevelAuditMode objectLevelAuditMode,
                        @Nullable Map<String, String> freeformTags,
                        @Nullable Map<String, Map<String, Object>> definedTags,
                        @Nullable String kmsKeyId,
                        boolean objectEventsEnabled) {
        this.namespaceName = Preconditions.checkNotNull(namespaceName);
        this.bucketName = Preconditions.checkNotNull(bucketName);
        this.compartmentId = Preconditions.checkNotNull(compartmentId);
        this.createdBy = Preconditions.checkNotNull(createdBy);
        this.metadata = metadata == null ? ImmutableMap.of() : ImmutableMap.copyOf(metadata);
        this.bucketPublicAccessType = bucketPublicAccessType;
        this.bucketStorageTier = bucketStorageTier;
        this.objectLevelAuditMode = objectLevelAuditMode;
        this.freeformTags = freeformTags;
        this.definedTags = definedTags;
        this.kmsKeyId = kmsKeyId;
        this.objectEventsEnabled = objectEventsEnabled;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getCompartmentId() {
        return compartmentId;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public BucketPublicAccessType getBucketPublicAccessType() {
        return bucketPublicAccessType;
    }

    public BucketStorageTier getBucketStorageTier() {
        return bucketStorageTier;
    }

    // Optional is not necessary after feature is public
    public Optional<ObjectLevelAuditMode> getObjectLevelAuditMode() {
        return Optional.ofNullable(objectLevelAuditMode);
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

    public boolean isObjectEventsEnabled() {
        return objectEventsEnabled;
    }

    public Map<String, Object> getOptions() {
        return ImmutableMap.of("objectEventsEnabled", objectEventsEnabled);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("namespaceName", namespaceName)
                .add("bucketName", bucketName)
                .add("compartmentId", compartmentId)
                .add("createdBy", createdBy)
                .add("metadata", metadata)
                .add("bucketPublicAccessType", bucketPublicAccessType)
                .add("bucketStorageTier", bucketStorageTier)
                .add("objectLevelAuditMode", objectLevelAuditMode)
                .add("freeformTags", freeformTags)
                .add("definedTags", definedTags)
                .add("kmsKeyId", kmsKeyId)
                .add("objectEventsEnabled", objectEventsEnabled)
                .toString();
    }
}
