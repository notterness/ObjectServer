package com.webutils.webserver.common;

public class ChunkDeleteInfo {

    private final String serverName;
    private final String chunkUID;

    public ChunkDeleteInfo(final String serverName, final String chunkUID) {
        this.serverName = serverName;
        this.chunkUID = chunkUID;
    }

    public String getChunkUID() { return chunkUID; }

    public String getServerName() {
        return serverName;
    }
}
