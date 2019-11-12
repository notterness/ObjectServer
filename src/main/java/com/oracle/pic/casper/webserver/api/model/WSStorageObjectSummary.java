package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.ObjectStorageTier;
import com.oracle.pic.casper.metadata.crypto.EncryptedEncryptionKey;
import com.oracle.pic.casper.metadata.crypto.EncryptedMetadata;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.ObjectId;
import com.oracle.pic.casper.objectmeta.ObjectKey;

import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import javax.annotation.Nullable;

public class WSStorageObjectSummary {
    /**
     * The key of the object.
     */
    private final ObjectKey key;

    /**
     * The object id.
     */
    private final ObjectId objectId;
    /**
     * The ETag of this version of the object.
     */
    private final String etag;
    /**
     * The time this object was created.
     */
    private final Date creationTime;
    /**
     * The time this object was modified.
     */
    private final Date modificationTime;
    /**
     * The total size of the object, in bytes.
     */
    private final long totalSizeInBytes;
    /**
     * The MD5 of the object as a base64-encoded string. This can be null.
     */
    @Nullable
    private final String md5;
    /**
     * The type of the checksum specified in the md5 field.
     */
    private final ChecksumType checksumType;
    /**
     * The user-defined object metadata encrypted
     */
    private final EncryptedMetadata encryptedMetadata;

    /**
     * The encrypted encryption key used to decrypt the encrypted metadata.
     */
    private final EncryptedEncryptionKey encryptedEncryptionKey;

    /**
     * The number of parts. (only used for {@link com.oracle.pic.casper.common.rest.CommonHeaders#CASPER_MULTIPART_MD5}
     * md5 hash construction.
     */
    private final int partCount;

    private final Optional<Date> minimumRetentionTime;

    /**
     * The time the archived object was last archived, or will be archived in the future.
     */
    private Date archivedTime = null;

    /**
     * The time the archived object was last restored, or will be restored in the future.
     */
    private Date restoredTime = null;

    /**
     * The archival state of object
     */
    private ArchivalState archivalState;

    /**
     * The storage tier of the object. This does not need to be final. As of now once set the
     * tier of the object cannot change on the fly but it might change in future with
     * archive object or object metadata update API calls. However this cannot be null
     * Callers must pass in the object tier while creating the objects even if the tier is
     * default STANDARD tier
     */
    private ObjectStorageTier storageTier;

    private final byte[] encryptionKeyPayload;
    private final String encryptionKeyVersion;
    private final String encryptedMetadataString;

    public WSStorageObjectSummary(ObjectKey key,
                                  ObjectId objectId,
                                  String etag,
                                  Date creationTime,
                                  Date modificationTime,
                                  long totalSizeInBytes,
                                  @Nullable String md5,
                                  ChecksumType checksumType,
                                  String encryptedMetadata,
                                  @Nullable byte[] encryptionKeyPayload,
                                  @Nullable String encryptionKeyVersion,
                                  int partCount,
                                  @Nullable Date minimumRetentionTime,
                                  @Nullable Date archivedTime,
                                  @Nullable Date restoredTime,
                                  ArchivalState archivalState,
                                  ObjectStorageTier objectStorageTier) {
        Preconditions.checkNotNull(creationTime, "creationTime");
        Preconditions.checkNotNull(modificationTime, "modificationTime");
        Preconditions.checkArgument(
                !creationTime.after(modificationTime),
                "Creation time can not be after modification time");
        Preconditions.checkArgument(totalSizeInBytes >= 0,
                "totalSizeInBytes must be >= 0 (%s)", totalSizeInBytes);
        Preconditions.checkNotNull(encryptedMetadata, "encryptedMetadata");
        Preconditions.checkArgument(restoredTime == null ||
                (archivedTime != null && !(archivedTime.before(restoredTime))));
        final EncryptedEncryptionKey encryptedEncryptionKey =
                new EncryptedEncryptionKey(encryptionKeyPayload, encryptionKeyVersion);

        this.encryptionKeyPayload = encryptionKeyPayload == null ? null : encryptionKeyPayload.clone();
        this.encryptionKeyVersion = encryptionKeyVersion;
        this.encryptedMetadataString = encryptedMetadata;

        this.archivalState = Preconditions.checkNotNull(archivalState);
        this.storageTier = Preconditions.checkNotNull(objectStorageTier);
        this.modificationTime = new Date(modificationTime.getTime());
        this.creationTime = new Date(creationTime.getTime());
        this.md5 = md5;
        this.checksumType = checksumType;
        this.encryptedMetadata = new EncryptedMetadata(encryptedMetadata, encryptedEncryptionKey);
        this.encryptedEncryptionKey = encryptedEncryptionKey;
        this.etag = Preconditions.checkNotNull(etag, "etag");
        this.totalSizeInBytes = totalSizeInBytes;
        this.key = Preconditions.checkNotNull(key, "key");
        this.objectId = Preconditions.checkNotNull(objectId);
        this.partCount = partCount;
        this.minimumRetentionTime = Optional.ofNullable(minimumRetentionTime);
        if (archivedTime != null) {
            this.archivedTime = new Date(archivedTime.getTime());
        }
        if (restoredTime != null) {
            this.restoredTime = new Date(restoredTime.getTime());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, objectId, etag, creationTime, modificationTime, totalSizeInBytes,
                md5, checksumType, encryptedMetadata, encryptedEncryptionKey, partCount, minimumRetentionTime,
                archivedTime, restoredTime, archivalState);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WSStorageObjectSummary)) {
            return false;
        }
        WSStorageObjectSummary that = (WSStorageObjectSummary) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(objectId, that.objectId) &&
                Objects.equals(etag, that.etag) &&
                Objects.equals(creationTime, that.creationTime) &&
                Objects.equals(modificationTime, that.modificationTime) &&
                totalSizeInBytes == that.totalSizeInBytes &&
                Objects.equals(md5, that.md5) &&
                checksumType == that.checksumType &&
                Objects.equals(encryptedMetadata, that.encryptedMetadata) &&
                Objects.equals(encryptedEncryptionKey, that.encryptedEncryptionKey) &&
                partCount == that.partCount &&
                Objects.equals(minimumRetentionTime, that.minimumRetentionTime) &&
                Objects.equals(archivedTime, that.archivedTime) &&
                Objects.equals(restoredTime, that.restoredTime) &&
                Objects.equals(archivalState, that.archivalState) &&
                Objects.equals(storageTier, that.storageTier);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("etag", etag)
                .add("creationTime", creationTime)
                .add("modificationTime", modificationTime)
                .add("totalSizeInBytes", totalSizeInBytes)
                .add("md5", md5)
                .add("checksumType", checksumType)
                .add("metadata", encryptedMetadata)
                .add("encryptedEncryptionKey", encryptedEncryptionKey)
                .add("partCount", partCount)
                .add("key", key)
                .add("objectId", objectId)
                .add("minimumRetentionTime", minimumRetentionTime.orElse(null))
                .add("archivedTime", archivedTime)
                .add("restoredTime", restoredTime)
                .add("archivalState", archivalState)
                .add("storageTier", storageTier)
                .toString();
    }

    /**
     * This method is only used in the headObject call path. HeadObject will never used the chunk map.
     *
     * @return a WSStorageObject with a fake Chunk map.
     */

    public WSStorageObject toWSStorageObjectWithEmptyChunk() {
        return new WSStorageObject(
            this.key,
            this.objectId,
            this.etag,
            this.creationTime,
            this.modificationTime,
            this.totalSizeInBytes,
            this.md5,
            this.checksumType,
            this.encryptedMetadataString,
            this.encryptionKeyPayload,
            this.encryptionKeyVersion,
            new TreeMap<>(),
            this.partCount,
            this.minimumRetentionTime.orElse(null),
            this.archivedTime,
            this.restoredTime,
            this.archivalState,
            this.storageTier);
    }

    /**
     * Gets the ETag of this version of the object.
     *
     * @return The ETag of this version of the object.
     */
    public String getETag() {
        return this.etag;
    }

    /**
     * Get the time this version was created (the commit time).
     *
     * @return The creation time of the object.
     */
    public Date getCreationTime() {
        return new Date(this.creationTime.getTime());
    }

    /**
     * Get the time this version was modified.
     *
     * @return The modification time of the object.
     */
    public Date getModificationTime() {
        return new Date(this.modificationTime.getTime());
    }

    /**
     * Gets the total size, in bytes, of this version of the object.
     *
     * @return The total size, in bytes, of the object's data.
     */
    public long getTotalSizeInBytes() {
        return this.totalSizeInBytes;
    }

    /**
     * Gets the MD5 hash of the the object (as a base-64 encoded value). This
     * value can be null.
     *
     * @return The MD5 hash of the object.
     */
    public String getMd5() {
        return this.md5;
    }

    /**
     * The type of checksum for the object stored in the MD5 field.
     */
    public ChecksumType getChecksumType() {
        return this.checksumType;
    }

    /**
     * Gets the encryption key used to decrypt the encrypted metadata.
     *
     * @return The encryption key.
     */
    public @Nullable EncryptedEncryptionKey getEncryptedEncryptionKey() {
        return this.encryptedEncryptionKey;
    }

    /**
     * Gets the user-defined decrypted object metadata.
     */
    public Map<String, String> getMetadata(DecidingKeyManagementService kms) {
        return this.encryptedMetadata.getMetadata(kms);
    }

    /**
     * Gets the number of parts used to create this object. (Only relevant for multipart object).
     *
     * @return The number of parts
     */
    public int getPartCount() {
        return partCount;
    }

    /**
     * Gets the key of the object.
     *
     * @return The key of the object.
     */
    public ObjectKey getKey() {
        return key;
    }

    /**
     * Returns the object id associated to the object.
     *
     * @return the object id
     */
    public ObjectId getObjectId() {
        return objectId;
    }

    /**
     *
     * @return the minimum retention time for an object where any modification on this object before this date will
     * incur a penalty. Parts do not have a minimum retention time nor are objects required to have one set.
     */
    public Optional<Date> getMinimumRetentionTime() {
        return minimumRetentionTime;
    }

    /**
     * Get the time the archived object is last archived, or will be archived in the future.
     * This value can be null if the object is not an archive object
     * @return The archived time of the object.
     */
    public Optional<Date> getArchivedTime() {
        return Optional.ofNullable(archivedTime);
    }

    /**
     * Get the time the archived object is last restored, or will be restored in the future.
     * This value can be null if the object is not an archive object
     * @return The restored time of the object.
     */
    public Optional<Date> getRestoredTime() {
        return Optional.ofNullable(restoredTime);
    }

    /**
     * Get the archival state of object
     * @return The archival state of object.
     */
    public ArchivalState getArchivalState() {
        return archivalState;
    }

    /**
     * Gets the storage tier of the object.\
     * @return The storage tier of the object.
     */
    public ObjectStorageTier getStorageTier() {
        return storageTier;
    }

    public boolean isGetPossible() {
        ArchivalState state = getArchivalState();
        return ((state == ArchivalState.Available) || (state == ArchivalState.Restored));
    }
}
