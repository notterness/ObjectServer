package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.ObjectStorageTier;
import com.oracle.pic.casper.metadata.crypto.EncryptedEncryptionKey;
import com.oracle.pic.casper.metadata.crypto.EncryptedMetadata;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.ObjectId;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;

import javax.annotation.Nullable;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class ObjectFullSummary implements ObjectBaseSummary {

    private final String objectName;
    private final ObjectId objectId;
    private final String namespace;
    private final String bucketName;
    private final Api api;
    private final String etag;
    private final Instant creationTime;
    private final Instant modificationTime;
    private final long totalSizeInBytes;
    private final String md5;
    private final ChecksumType checksumType;
    private final int partCount;
    private final EncryptedMetadata encryptedMetadata;
    private final EncryptedEncryptionKey encryptedEncryptionKey;
    private final Instant minimumRetentionTime;
    private final Instant archivedTime;
    private final Instant restoredTime;
    private final ArchivalState archivalState;
    private final ObjectStorageTier storageTier;

    public ObjectFullSummary(String objectName,
                             ObjectId objectId,
                             String namespace,
                             String bucketName,
                             Api api,
                             String etag,
                             Instant creationTime,
                             Instant modificationTime,
                             long totalSizeInBytes,
                             @Nullable String md5,
                             ChecksumType checksumType,
                             int partCount,
                             String encryptedMetadata,
                             @Nullable byte[] encryptionKeyPayload,
                             @Nullable String encryptionKeyVersion,
                             @Nullable Instant minimumRetentionTime,
                             @Nullable Instant archivedTime,
                             @Nullable Instant restoredTime,
                             ArchivalState archivalState,
                             ObjectStorageTier storageTier) {
        this.objectName = Preconditions.checkNotNull(objectName);
        this.objectId = Preconditions.checkNotNull(objectId);
        this.namespace = Preconditions.checkNotNull(namespace);
        this.bucketName = Preconditions.checkNotNull(bucketName);
        this.api = Preconditions.checkNotNull(api);
        this.etag = Preconditions.checkNotNull(etag);
        this.creationTime = Preconditions.checkNotNull(creationTime);
        this.modificationTime = Preconditions.checkNotNull(modificationTime);
        this.totalSizeInBytes = totalSizeInBytes;
        this.md5 = md5;
        this.checksumType = Preconditions.checkNotNull(checksumType);
        this.partCount = partCount;
        final EncryptedEncryptionKey encryptedEncryptionKey =
                new EncryptedEncryptionKey(encryptionKeyPayload, encryptionKeyVersion);
        this.encryptedEncryptionKey = encryptedEncryptionKey;
        this.encryptedMetadata = new EncryptedMetadata(encryptedMetadata, encryptedEncryptionKey);
        this.minimumRetentionTime = minimumRetentionTime;
        this.archivedTime = archivedTime;
        this.restoredTime = restoredTime;
        this.archivalState = Preconditions.checkNotNull(archivalState);
        this.storageTier = Preconditions.checkNotNull(storageTier);
    }


    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public ObjectSummary makeSummary(List<ObjectProperties> properties, DecidingKeyManagementService kms) {
        ObjectSummary objectSummary =  new ObjectSummary();
        for (ObjectProperties op : properties) {
            switch (op) {

                case NAME: objectSummary.setName(objectName); break;
                case SIZE: objectSummary.setSize(totalSizeInBytes); break;
                case MD5:
                    Checksum checksum = checksumType == ChecksumType.MD5 ?
                            Checksum.fromBase64(md5) :
                            Checksum.fromMultipartBase64(md5,
                                    partCount);
                    objectSummary.setChecksum(checksum);
                    break;
                case TIMECREATED: objectSummary.setTimeCreated(Date.from(creationTime)); break;
                case TIMEMODIFIED: objectSummary.setTimeModified(Date.from(modificationTime)); break;
                case METADATA: objectSummary.setMetadata(encryptedMetadata.getMetadata(kms)); break;
                case ETAG: objectSummary.setETag(etag); break;
                case ARCHIVALSTATE: objectSummary.setArchivalState(archivalState); break;
                case ARCHIVED:
                    // There is a bug where we are still requesting the deprecated ARCHIVED property. We currently
                    // don't do anything when this property is requested. CASPER-6472
                    break;
                default:
                    // Since we don't pre-validate object properties for listing full objects,
                    // just continue if there is a property we don't recognize.
                    break;
            }
        }
        return objectSummary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectFullSummary that = (ObjectFullSummary) o;
        return totalSizeInBytes == that.totalSizeInBytes &&
                partCount == that.partCount &&
                Objects.equals(objectName, that.objectName) &&
                Objects.equals(objectId, that.objectId) &&
                Objects.equals(namespace, that.namespace) &&
                Objects.equals(bucketName, that.bucketName) &&
                api == that.api &&
                Objects.equals(etag, that.etag) &&
                Objects.equals(creationTime, that.creationTime) &&
                Objects.equals(modificationTime, that.modificationTime) &&
                Objects.equals(md5, that.md5) &&
                checksumType == that.checksumType &&
                Objects.equals(encryptedMetadata, that.encryptedMetadata) &&
                Objects.equals(encryptedEncryptionKey, that.encryptedEncryptionKey) &&
                Objects.equals(minimumRetentionTime, that.minimumRetentionTime) &&
                Objects.equals(archivedTime, that.archivedTime) &&
                Objects.equals(restoredTime, that.restoredTime) &&
                archivalState == that.archivalState &&
                storageTier == that.storageTier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectName, objectId, namespace, bucketName, api, etag, creationTime, modificationTime,
                totalSizeInBytes, md5, checksumType, partCount, encryptedMetadata,
                encryptedEncryptionKey, minimumRetentionTime, archivedTime, restoredTime, archivalState, storageTier);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("objectName", objectName)
                .add("objectId", objectId)
                .add("namespace", namespace)
                .add("bucketName", bucketName)
                .add("api", api)
                .add("etag", etag)
                .add("creationTime", creationTime)
                .add("modificationTime", modificationTime)
                .add("totalSizeInBytes", totalSizeInBytes)
                .add("md5", md5)
                .add("checksumType", checksumType)
                .add("partCount", partCount)
                .add("encryptedMetadata", encryptedMetadata)
                .add("encryptedEncryptionKey", encryptedEncryptionKey)
                .add("minimumRetentionTime", minimumRetentionTime)
                .add("archivedTime", archivedTime)
                .add("restoredTime", restoredTime)
                .add("archivalState", archivalState)
                .add("storageTier", storageTier)
                .toString();
    }
}
