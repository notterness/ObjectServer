package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.ObjectStorageTier;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.objectmeta.ObjectId;
import com.oracle.pic.casper.objectmeta.ObjectKey;

import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import javax.annotation.Nullable;

public class WSStorageObject extends WSStorageObjectSummary {

    /**
     * Information about the objectChunks of this object.
     */
    private final NavigableMap<Long, WSStorageObjectChunk> wsStorageObjectChunks;

    public WSStorageObject(ObjectKey key,
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
                           Map<Long, WSStorageObjectChunk> objectChunks,
                           int partCount,
                           @Nullable Date minimumRetentionTime,
                           @Nullable Date archivedTime,
                           @Nullable Date restoredTime,
                           ArchivalState archivalState,
                           ObjectStorageTier objectStorageTier) {
        super(key, objectId, etag, creationTime, modificationTime, totalSizeInBytes,
              md5, checksumType, encryptedMetadata, encryptionKeyPayload, encryptionKeyVersion, partCount,
              minimumRetentionTime, archivedTime, restoredTime, archivalState, objectStorageTier);
        Preconditions.checkNotNull(objectChunks, "objectChunks");

        // Usually ObjectChunks is not empty.
        // ObjectChunks is empty (1) if it is in headObject call path because headObject doesn't need chunk info.
        // (2) if the object is a 0-byte object
        if (!objectChunks.isEmpty()) {
            // Look for null values in the chunk map
            objectChunks.values().stream().forEach(Preconditions::checkNotNull);
            final long expectedTotalSizeInBytes = objectChunks.values().stream()
                .mapToLong(WSStorageObjectChunk::getSizeInBytes).sum();
            Preconditions.checkArgument(
                expectedTotalSizeInBytes == this.getTotalSizeInBytes(),
                "Chunks have a different total size (%s) than totalSizeInBytes (%s)",
                expectedTotalSizeInBytes,
                totalSizeInBytes);
        }

        this.wsStorageObjectChunks = ImmutableSortedMap.copyOf(objectChunks);

        if (!this.wsStorageObjectChunks.isEmpty()) {
            Preconditions.checkArgument(
                    this.wsStorageObjectChunks.firstKey() == 0,
                    "First chunk offset (%s) should be 0",
                    this.wsStorageObjectChunks.firstKey());
        }
    }

    /**
     * Gets a map of ObjectChunks indexed by the first byte offset of the
     * chunk. The first entry of this map will always be offset 0.
     *
     * @return A map of initial-offset -> chunk for the object's data.
     */
    public NavigableMap<Long, WSStorageObjectChunk> getObjectChunks() {
        return this.wsStorageObjectChunks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WSStorageObject)) return false;
        if (!super.equals(o)) return false;
        WSStorageObject that = (WSStorageObject) o;
        return Objects.equals(wsStorageObjectChunks, that.wsStorageObjectChunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), wsStorageObjectChunks);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("objectChunks", wsStorageObjectChunks)
                .add("-superClass-", super.toString())
                .toString();
    }
}
