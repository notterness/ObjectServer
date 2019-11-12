package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.metadata.crypto.EncryptedEncryptionKey;

import javax.annotation.Nullable;

public class WSStorageObjectChunk {
    /**
     * The size of the data, in bytes.
     */
    private final long sizeInBytes;

    /**
     * The volume containing the data.
     */
    private final long volumeId;

    /**
     * The ID of the chunk inside of the volume.
     */
    private final int volumeObjectNumber;

    /**
     * The encrypted encryption key that was used for this chunk.
     * Could be null if it was created before encryption at
     * rest was added.
     */
    private final EncryptedEncryptionKey encryptedEncryptionKey;

    /**
     * Constructs a new WSStorageObjectChunk.
     *
     * @param sizeInBytes The size of the chunk's data.
     * @param volumeId Volume that stores the chunk's data.
     * @param volumeObjectNumber VON of the chunk's data inside the volume.
     * @param encryptionKeyPayload The encrypted encryption key payload used to
     *                               encrypt the chunk on the storage.
     * @param encryptionKeyVersion The encrypted encryption key version.
     */
    public WSStorageObjectChunk(long sizeInBytes, long volumeId,
                                int volumeObjectNumber, @Nullable byte[] encryptionKeyPayload,
                                @Nullable String encryptionKeyVersion) {
        Preconditions.checkArgument(sizeInBytes >= 0, "sizeInBytes(%s) must be >= 0", sizeInBytes);
        Preconditions.checkArgument(volumeId >= 0, "volumeId(%s) must be >= 0", volumeId);
        Preconditions.checkArgument(
                volumeObjectNumber >= 0,
                "volumeObjectNumber(%s) must be >= 0", volumeObjectNumber
        );
        this.sizeInBytes = sizeInBytes;
        this.volumeId = volumeId;
        this.volumeObjectNumber = volumeObjectNumber;
        this.encryptedEncryptionKey = new EncryptedEncryptionKey(encryptionKeyPayload, encryptionKeyVersion);
    }

    /**
     * Get the encrypted encryption key that was used to encrypt this chunk.
     * May be null if the chunk was created before encryption at rest was implemented.
     */
    public @Nullable EncryptedEncryptionKey getEncryptedEncryptionKey() {
        return this.encryptedEncryptionKey;
    }

    /**
     * Gets the size of the chunk, in bytes.
     *
     * @return Size of the chunk's data, in bytes.
     */
    public long getSizeInBytes() {
        return this.sizeInBytes;
    }

    /**
     * Gets the volume ID of the chunk.
     *
     * @return Volume ID of the chunk's data.
     */
    public long getVolumeId() {
        return this.volumeId;
    }

    /**
     * Gets the volume object number of the chunk.
     *
     * @return VON of the chunk's data.
     */
    public int getVolumeObjectNumber() {
        return this.volumeObjectNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WSStorageObjectChunk that = (WSStorageObjectChunk) o;
        return sizeInBytes == that.sizeInBytes &&
                volumeId == that.volumeId &&
                volumeObjectNumber == that.volumeObjectNumber &&
                Objects.equal(encryptedEncryptionKey, that.encryptedEncryptionKey);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sizeInBytes, volumeId, volumeObjectNumber, encryptedEncryptionKey);
    }

    @Override
    public String toString() {

        return String.format("%d bytes@%s:%s (encryptedEncryptionKey=%s)",
                this.sizeInBytes,
                this.volumeId,
                this.volumeObjectNumber,
                this.encryptedEncryptionKey);
    }
}
