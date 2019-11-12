package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.common.encryption.EncryptionKey;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.BucketMeterFlagSet;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.BucketStorageTier;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.common.replication.ReplicationOptions;
import com.oracle.pic.casper.mds.MdsBucketToken;
import com.oracle.pic.casper.metadata.crypto.EncryptedEncryptionKey;
import com.oracle.pic.casper.metadata.crypto.EncryptedMetadata;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.model.serde.OptionsSerialization;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class WSTenantBucketInfo extends WSTenantBucketSummary {

    /**
     * The OCID of the bucket
     */
    private final String ocid;

    /**
     * Time this bucket was modified.
     */
    private final Instant modificationTime;

    /**
     * Time this bucket was deleted.
     */
    private final Instant deletionTime;

    /**
     * User defined metadata associated with the bucket
     */
    private final EncryptedMetadata encryptedMetadata;


    /**
     * The encrypted encryption key used for bucket metadata and tags encryption
     */
    private final EncryptedEncryptionKey encryptedEncryptionKey;

    /**
     * The object access policy.
     */
    private final BucketPublicAccessType publicAccessType;

    /**
     * The type of storage tier for the bucket (Standard /archive).
     */
    private final BucketStorageTier storageTier;

    /**
     * Additional meter settings control Bling meters for this bucket.
     */
    private final BucketMeterFlagSet meterFlagSet;

    /**
     * Serialized representation of the serializedOptions for this bucket (only for internal use).
     */
    private final String serializedOptions;

    /**
     * The object-level audit logging mode of the bucket.
     */
    private final ObjectLevelAuditMode objectLevelAuditMode;

    /**
     * The KMS key id of the bucket, used to generate encryption key from KMS
     */
    private final String kmsKeyId;

    /**
     * This ID is used to generate the PAR prefix so it needs to be immutable. For buckets with existing PARs, the
     * prefix is the original bucketId. For now, newly created buckets will also use the original bucketId.
     * If the value in the DB is null, we default this to be the bucketId
     */
    private final String immutableResourceId;

    private final MdsBucketToken mdsBucketToken;

    private final boolean isTenancyDeleted;

    private final boolean cacheable;

    private static final Logger LOG = LoggerFactory.getLogger(WSTenantBucketInfo.class);

    public WSTenantBucketInfo(String ocid,
                              NamespaceKey namespaceKey,
                              String bucketName,
                              String compartment,
                              String creationUser,
                              Instant creationTime,
                              Instant modificationTime,
                              Instant deletionTime,
                              String etag,
                              @Nullable String encryptedMetadata,
                              @Nullable byte[] encryptionKeyPayload,
                              @Nullable String encryptionKeyVersion,
                              BucketPublicAccessType publicAccessType,
                              BucketStorageTier storageTier,
                              BucketMeterFlagSet meterFlagSet,
                              @Nullable String serializedOptions,
                              ObjectLevelAuditMode objectLevelAuditMode,
                              String serializedTags,
                              @Nullable String kmsKeyId,
                              @Nullable String objectLifecyclePolicyEtag,
                              String immutableResourceId,
                              MdsBucketToken mdsBucketToken,
                              boolean isTenancyDeleted,
                              boolean cacheable) {
        super(namespaceKey, bucketName, compartment, creationUser, creationTime,
                etag, objectLifecyclePolicyEtag, serializedTags);
        this.ocid = ocid;

        // TODO: Once we make these columns required fields, add the check null conditions.
        /**
         this.metadata = Preconditions.checkNotNull(metadata);
         this.publicAccessType = Preconditions.checkNotNull(publicAccessType);
         this.storageTier = Preconditions.checkNotNull(storageTier);
         this.immutableResourceId = Preconditions.checkNotNull(immutableResourceId);
         */
        this.modificationTime = modificationTime;
        this.deletionTime = deletionTime;
        this.encryptedEncryptionKey = new EncryptedEncryptionKey(encryptionKeyPayload, encryptionKeyVersion);
        this.encryptedMetadata = (encryptedMetadata == null ?
                null : new EncryptedMetadata(encryptedMetadata, encryptedEncryptionKey));
        this.publicAccessType = publicAccessType;
        this.storageTier = storageTier;
        this.meterFlagSet = meterFlagSet;
        this.serializedOptions = serializedOptions;
        this.objectLevelAuditMode = objectLevelAuditMode;
        this.kmsKeyId = kmsKeyId;
        this.immutableResourceId = immutableResourceId;
        // Verify that bucketShards is not null and not empty
        this.mdsBucketToken = Preconditions.checkNotNull(mdsBucketToken);
        this.isTenancyDeleted = isTenancyDeleted;
        this.cacheable = cacheable;
    }

    public String getOcid() {
        return ocid;
    }

    public Instant getModificationTime() {
        return modificationTime;
    }

    public Instant getDeletionTime() {
        return deletionTime;
    }

    public EncryptedEncryptionKey getEncryptedEncryptionKey() {
        return encryptedEncryptionKey;
    }

    public EncryptionKey getEncryptionKey(DecidingKeyManagementService kms) {
        return encryptedEncryptionKey.getEncryptionKey(kms);
    }

    public Map<String, String> getMetadata(DecidingKeyManagementService kms) {
        return getMetadataHelper(kms, false);
    }

    public Map<String, String> getMetadataIgnoreDecryptError(DecidingKeyManagementService kms) {
        return getMetadataHelper(kms, true);
    }

    private Map<String, String> getMetadataHelper(DecidingKeyManagementService kms, boolean ignoreDecryptError) {
        try {
            return this.encryptedMetadata == null ? ImmutableMap.of() : this.encryptedMetadata.getMetadata(kms);
        } catch (Exception ex) {
            if (ignoreDecryptError) {
                LOG.error("Failed to decrypt bucket metadata, will return empty", ex);
                return ImmutableMap.of();
            } else {
                throw ex;
            }
        }
    }

    public EncryptedMetadata getEncryptedMetadata() {
        return this.encryptedMetadata;
    }

    public BucketPublicAccessType getPublicAccessType() {
        return publicAccessType;
    }

    public BucketStorageTier getStorageTier() {
        return storageTier;
    }

    public ObjectLevelAuditMode getObjectLevelAuditMode() {
        return objectLevelAuditMode;
    }

    public Optional<String> getKmsKeyId() {
        if (Strings.isNullOrEmpty(kmsKeyId)) {
            return Optional.empty();
        }
        return Optional.of(kmsKeyId);
    }

    public String getImmutableResourceId() {
        return immutableResourceId;
    }

    public BucketMeterFlagSet getMeterFlagSet() {
        return meterFlagSet;
    }

    public Optional<String> getSerializedOptions() {
        if (Strings.isNullOrEmpty(serializedOptions)) {
            return Optional.empty();
        }
        return Optional.of(serializedOptions);
    }

    public Map<String, Object> getOptions() {
        return OptionsSerialization.deserializeOptions(getSerializedOptions().orElse(null));
    }

    public boolean isReadOnly() {
        Map<String, Object> options = getOptions();
        return options != null &&
                options.get(ReplicationOptions.XRR_POLICY_ID) != null &&
                options.get(ReplicationOptions.XRR_POLICY_NAME) != null &&
                options.get(ReplicationOptions.XRR_SOURCE_REGION) != null &&
                options.get(ReplicationOptions.XRR_SOURCE_BUCKET) != null &&
                options.get(ReplicationOptions.XRR_SOURCE_BUCKET_ID) != null;
    }

    public boolean isReplicationEnabled() {
        Map<String, Object> options = getOptions();
        return options != null &&
                options.get(ReplicationOptions.XRR_POLICY_ID) != null &&
                options.get(ReplicationOptions.XRR_POLICY_NAME) == null &&
                options.get(ReplicationOptions.XRR_SOURCE_REGION) == null &&
                options.get(ReplicationOptions.XRR_SOURCE_BUCKET) == null &&
                options.get(ReplicationOptions.XRR_SOURCE_BUCKET_ID) == null;
    }

    public MdsBucketToken getMdsBucketToken() {
        return mdsBucketToken;
    }

    public boolean isTenancyDeleted() {
        return isTenancyDeleted;
    }

    public boolean isObjectEventsEnabled() {
        return Optional.ofNullable((Boolean) getOptions().get(BucketOptions.EVENTS_TOGGLE.getValue()))
                .orElse(false);
    }

    public boolean isCacheable() {
        return cacheable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        WSTenantBucketInfo that = (WSTenantBucketInfo) o;
        return Objects.equals(modificationTime, that.modificationTime) &&
                Objects.equals(encryptedEncryptionKey, that.encryptedEncryptionKey) &&
                Objects.equals(encryptedMetadata, that.encryptedMetadata) &&
                publicAccessType == that.publicAccessType && storageTier == that.storageTier &&
                Objects.equals(meterFlagSet, that.meterFlagSet) && objectLevelAuditMode == that.objectLevelAuditMode &&
                Objects.equals(serializedOptions, that.serializedOptions) &&
                Objects.equals(kmsKeyId, that.kmsKeyId) &&
                Objects.equals(immutableResourceId, that.immutableResourceId) &&
                Objects.equals(mdsBucketToken, that.mdsBucketToken) &&
                isTenancyDeleted == that.isTenancyDeleted &&
                cacheable == that.cacheable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                ocid,
                modificationTime,
                encryptedEncryptionKey,
                encryptedMetadata,
                publicAccessType,
                storageTier,
                meterFlagSet,
                serializedOptions,
                objectLevelAuditMode,
                kmsKeyId,
                immutableResourceId,
                mdsBucketToken,
                isTenancyDeleted,
                cacheable);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("ocid", ocid)
                .add("modificationTime", modificationTime)
                .add("deletionTime", deletionTime)
                .add("encryptedEncryptionKey", encryptedEncryptionKey)
                .add("encryptedMetadata", encryptedMetadata)
                .add("publicAccessType", publicAccessType)
                .add("storageTier", storageTier)
                .add("meterFlagSet", meterFlagSet)
                .add("serializedOptions", serializedOptions)
                .add("objectLevelAuditMode", objectLevelAuditMode)
                .add("kmsKeyId", kmsKeyId)
                .add("immutableResourceId", immutableResourceId)
                .add("mdsBucketToken", mdsBucketToken)
                .add("-superClass-", super.toString())
                .add("isTenancyDeleted", isTenancyDeleted)
                .add("cacheable", cacheable)
                .toString();
    }

    public static final class Builder {
        private String ocid;
        private NamespaceKey namespaceKey;
        private String bucketName;
        private String compartment;
        private String creationUser;
        private Instant creationTime;
        private Instant modificationTime;
        private Instant deletionTime;
        private String etag;
        private String encryptedMetadata;
        private byte[] encryptionKeyPayload;
        private String encryptionKeyVersion;
        private BucketPublicAccessType publicAccessType;
        private BucketStorageTier storageTier;
        private ObjectLevelAuditMode objectLevelAuditMode;
        private String serializedTags;
        private String kmsKeyId;
        private String objectLifecyclePolicyEtag;
        private String immutableResourceId;
        private BucketMeterFlagSet meterFlagSet;
        private String serializedOptions;
        private MdsBucketToken mdsBucketToken;
        private boolean isTenancyDeleted;
        private boolean cacheable;

        private Builder() {
        }

        public static WSTenantBucketInfo.Builder builder() {
            return new WSTenantBucketInfo.Builder();
        }

        public WSTenantBucketInfo.Builder ocid(String ocid) {
            this.ocid = ocid;
            return this;
        }

        public WSTenantBucketInfo.Builder namespaceKey(NamespaceKey namespaceKey) {
            this.namespaceKey = namespaceKey;
            return this;
        }

        public WSTenantBucketInfo.Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public WSTenantBucketInfo.Builder compartment(String compartment) {
            this.compartment = compartment;
            return this;
        }

        public WSTenantBucketInfo.Builder creationUser(String creationUser) {
            this.creationUser = creationUser;
            return this;
        }

        public WSTenantBucketInfo.Builder creationTime(Instant creationTime) {
            this.creationTime = creationTime;
            return this;
        }

        public WSTenantBucketInfo.Builder modificationTime(Instant modificationTime) {
            this.modificationTime = modificationTime;
            return this;
        }

        public WSTenantBucketInfo.Builder deletionTime(Instant deletionTime) {
            this.deletionTime = deletionTime;
            return this;
        }

        public WSTenantBucketInfo.Builder etag(String etag) {
            this.etag = etag;
            return this;
        }

        @SuppressFBWarnings("EI_EXPOSE_REP2")
        public WSTenantBucketInfo.Builder encryptionKeyPayload(byte[] encryptionKeyPayload) {
            this.encryptionKeyPayload = encryptionKeyPayload;
            return this;
        }

        public WSTenantBucketInfo.Builder encryptionKeyVersion(String encryptionKeyVersion) {
            this.encryptionKeyVersion = encryptionKeyVersion;
            return this;
        }

        public WSTenantBucketInfo.Builder encryptedMetadata(String encryptedMetadata) {
            this.encryptedMetadata = encryptedMetadata;
            return this;
        }

        public WSTenantBucketInfo.Builder publicAccessType(BucketPublicAccessType publicAccessType) {
            this.publicAccessType = publicAccessType;
            return this;
        }

        public WSTenantBucketInfo.Builder storageTier(BucketStorageTier storageTier) {
            this.storageTier = storageTier;
            return this;
        }

        public WSTenantBucketInfo.Builder objectLevelAuditMode(ObjectLevelAuditMode objectLevelAuditMode) {
            this.objectLevelAuditMode = objectLevelAuditMode;
            return this;
        }

        public WSTenantBucketInfo.Builder serializedTags(String serializedTags) {
            this.serializedTags = serializedTags;
            return this;
        }

        public WSTenantBucketInfo.Builder kmsKeyId(String kmsKeyId) {
            this.kmsKeyId = kmsKeyId;
            return this;
        }

        public WSTenantBucketInfo.Builder objectLifecyclePolicyEtag(String objectLifecyclePolicyEtag) {
            this.objectLifecyclePolicyEtag = objectLifecyclePolicyEtag;
            return this;
        }

        public WSTenantBucketInfo.Builder immutableResourceId(String immutableResourceId) {
            this.immutableResourceId = immutableResourceId;
            return this;
        }

        public WSTenantBucketInfo.Builder meterFlagSet(BucketMeterFlagSet meterFlagSet) {
            this.meterFlagSet = meterFlagSet;
            return this;
        }

        public WSTenantBucketInfo.Builder serializedOptions(String serializedOptions) {
            this.serializedOptions = serializedOptions;
            return this;
        }

        public WSTenantBucketInfo.Builder mdsBucketToken(MdsBucketToken mdsBucketToken) {
            this.mdsBucketToken = mdsBucketToken;
            return this;
        }

        public WSTenantBucketInfo.Builder isTenancyDeleted(boolean isTenancyDeleted) {
            this.isTenancyDeleted = isTenancyDeleted;
            return this;
        }

        public WSTenantBucketInfo.Builder isCacheable(boolean cacheable) {
            this.cacheable = cacheable;
            return this;
        }

        public WSTenantBucketInfo build() {
            WSTenantBucketInfo wsTenantBucketInfo = new WSTenantBucketInfo(
                    ocid,
                    namespaceKey,
                    bucketName,
                    compartment,
                    creationUser,
                    creationTime,
                    modificationTime,
                    deletionTime,
                    etag,
                    encryptedMetadata,
                    encryptionKeyPayload,
                    encryptionKeyVersion,
                    publicAccessType,
                    storageTier,
                    meterFlagSet,
                    serializedOptions,
                    objectLevelAuditMode,
                    serializedTags,
                    kmsKeyId,
                    objectLifecyclePolicyEtag,
                    immutableResourceId,
                    mdsBucketToken,
                    isTenancyDeleted,
                    cacheable);
            return wsTenantBucketInfo;
        }
    }
}
