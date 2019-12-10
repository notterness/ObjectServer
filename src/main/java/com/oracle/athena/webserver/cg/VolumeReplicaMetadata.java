package com.oracle.pic.casper.webserver.api.backend.putobject;

import com.oracle.pic.casper.volumemeta.models.StorageServerInfo;

import java.util.List;

public final class VolumeReplicaMetadata {
    private final int volumeId;
    private final int von;
    private final List<StorageServerInfo> storageServers;

    public VolumeReplicaMetadata(int volumeId, int von, List<StorageServerInfo> storageServers) {
        this.volumeId = volumeId;
        this.von = von;
        this.storageServers = storageServers;
    }

    public int getVolumeId() {
        return volumeId;
    }

    public int getVon() {
        return von;
    }

    public List<StorageServerInfo> getStorageServers() {
        return storageServers;
    }

    @Override
    public String toString() {
        return "VolumeReplicaMetadata{" +
                "volumeId=" + volumeId +
                ", von=" + von +
                ", storageServers=" + storageServers +
                '}';
    }
}
