package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.oracle.pic.casper.metadata.serialization.TagSerialization;
import com.oracle.pic.casper.objectmeta.BucketKey;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.tagging.client.tag.TagSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

public class WSTenantBucketSummary {

    /**
     * The namespace that owns the bucket.
     */
    private final NamespaceKey namespaceKey;

    /**
     * Name of the bucket. Picked by the client.
     */
    private final String bucketName;

    /**
     * Compartment that contains the bucket.
     */
    private final String compartment;

    /**
     * The user that created the bucket
     */
    private final String creationUser;

    /**
     * Time this bucket was created.
     */
    private final Instant creationTime;

    /**
     * A GUID for optimistic locking
     */
    private final String etag;

    /**
     * The tags for this bucket
     */
    private final TagSet tagSet;

    /**
     * Object lifecycle policy ETag
     */
    @Nullable
    private final String objectLifecyclePolicyEtag;

    private static final Logger LOG = LoggerFactory.getLogger(WSTenantBucketSummary.class);

    public WSTenantBucketSummary(NamespaceKey namespaceKey,
                                 String bucketName,
                                 String compartment,
                                 String creationUser,
                                 Instant creationTime,
                                 String etag,
                                 @Nullable String objectLifecyclePolicyEtag,
                                 @Nullable String serializedTags) {
        // TODO: Once we make these columns required fields, add the check null conditions.
        /**
         this.modificationTime = Preconditions.checkNotNull(modificationTime);
         this.etag = Preconditions.checkNotNull(etag);
         Preconditions.checkNotNull(encryptionKeyPayload);
         Preconditions.checkNotNull(encryptionKeyVersion);
         */
        this.namespaceKey = Preconditions.checkNotNull(namespaceKey);
        this.bucketName = Preconditions.checkNotNull(bucketName);
        this.compartment = Preconditions.checkNotNull(compartment);
        this.creationUser = Preconditions.checkNotNull(creationUser);
        this.creationTime = Preconditions.checkNotNull(creationTime);
        // TODO: Once we make etag a required field, add the check null conditions.
        // this.etag = Preconditions.checkNotNull(etag);
        this.etag = etag;
        this.objectLifecyclePolicyEtag = objectLifecyclePolicyEtag;
        this.tagSet = TagSerialization.decodeTags(serializedTags);
    }

    public NamespaceKey getNamespaceKey() {
        return namespaceKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getCompartment() {
        return compartment;
    }

    public String getCreationUser() {
        return creationUser;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public String getEtag() {
        return etag;
    }

    public Optional<String> getObjectLifecyclePolicyEtag() {
        if (Strings.isNullOrEmpty(objectLifecyclePolicyEtag)) {
            return Optional.empty();
        }
        return Optional.of(objectLifecyclePolicyEtag);
    }

    public TagSet getTags() {
        return this.tagSet;
    }

    // TODO (multi-shard buckets): Deprecating this because each shard will have a different bucketKey
    public BucketKey getKey() {
        return new BucketKey(namespaceKey.getName(), getBucketName(), namespaceKey.getApi());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("namespaceKey", namespaceKey)
                .add("name", bucketName)
                .add("compartment", compartment)
                .add("creationUser", creationUser)
                .add("creationTime", creationTime)
                .add("etag", etag)
                .add("objectLifecyclePolicyEtag", objectLifecyclePolicyEtag)
                .add("tagSet", tagSet)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WSTenantBucketSummary that = (WSTenantBucketSummary) o;
        return Objects.equals(namespaceKey, that.namespaceKey) && Objects.equals(bucketName, that.bucketName) &&
                Objects.equals(compartment, that.compartment) && Objects.equals(creationUser, that.creationUser) &&
                Objects.equals(creationTime, that.creationTime) && Objects.equals(etag, that.etag) &&
                Objects.equals(objectLifecyclePolicyEtag, that.objectLifecyclePolicyEtag) &&
                Objects.equals(tagSet, that.tagSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespaceKey, bucketName, compartment, creationUser, creationTime,
                etag, objectLifecyclePolicyEtag, tagSet);
    }

    public static final class Builder {
        private NamespaceKey namespaceKey;
        private String bucketName;
        private String compartment;
        private String creationUser;
        private Instant creationTime;
        private String etag;
        private String objectLifecyclePolicyEtag;
        private String serializedTags;

        private Builder() {
        }

        public static WSTenantBucketSummary.Builder builder() {
            return new WSTenantBucketSummary.Builder();
        }

        public WSTenantBucketSummary.Builder namespaceKey(NamespaceKey namespaceKey) {
            this.namespaceKey = namespaceKey;
            return this;
        }

        public WSTenantBucketSummary.Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public WSTenantBucketSummary.Builder compartment(String compartment) {
            this.compartment = compartment;
            return this;
        }

        public WSTenantBucketSummary.Builder creationUser(String creationUser) {
            this.creationUser = creationUser;
            return this;
        }

        public WSTenantBucketSummary.Builder creationTime(Instant creationTime) {
            this.creationTime = creationTime;
            return this;
        }

        public WSTenantBucketSummary.Builder etag(String etag) {
            this.etag = etag;
            return this;
        }

        public WSTenantBucketSummary.Builder objectLifecyclePolicyEtag(String objectLifecyclePolicyEtag) {
            this.objectLifecyclePolicyEtag = objectLifecyclePolicyEtag;
            return this;
        }

        public WSTenantBucketSummary.Builder serializedTags(String serializedTags) {
            this.serializedTags = serializedTags;
            return this;
        }

        public WSTenantBucketSummary build() {
            WSTenantBucketSummary wsTenantBucketSummary = new WSTenantBucketSummary(
                    namespaceKey,
                    bucketName,
                    compartment,
                    creationUser,
                    creationTime,
                    etag,
                    objectLifecyclePolicyEtag,
                    serializedTags);
            return wsTenantBucketSummary;
        }
    }
}
