package com.webutils.webserver.http;

public class ListChunkInfo {

    private final int id;
    private final int index;
    private final String uid;
    private final int lba;
    private final int size;

    private final ChunkStatusEnum state;

    private final String lastAllocateTime;
    private final String lastDeleteTime;

    private final int serverId;

    public ListChunkInfo(final int id, final int index, final String uid, final int lba, final int size,
                         final ChunkStatusEnum state, final String lastAllocateTime, final String lastDeleteTime,
                         final int serverId) {

        this.id = id;
        this.index = index;

        this.uid = uid;

        this.lba = lba;
        this.size = size;

        this.state = state;

        this.lastAllocateTime = lastAllocateTime;
        this.lastDeleteTime = lastDeleteTime;

        this.serverId = serverId;
    }

    public String buildChunkListResponse(final boolean initialEntry, final boolean lastEntry) {
        StringBuilder body = new StringBuilder();

        if (initialEntry) {
            body.append("{\r\n  \"data\": [\r\n");
        }
        body.append("  \"chunk-").append(index).append("\":\n");
        body.append("    {\n");
        body.append("       \"" + HttpInfo.CHUNK_ID + "\": \"").append(id).append("\"\n");
        body.append("       \"" + ContentParser.CHUNK_UID + "\": \"").append(uid).append("\"\n");
        body.append("       \"" + HttpInfo.CHUNK_LBA + "\": \"").append(lba).append("\"\n");
        body.append("       \"chunk-size\": \"").append(size).append("\"\n");
        body.append("       \"chunk-state\": \"").append(state.toString()).append("\"\n");
        body.append("       \"last-allocated\": \"").append(lastAllocateTime).append("\"\n");
        body.append("       \"last-deleted\": \"").append(lastDeleteTime).append("\"\n");
        body.append("       \"" + ContentParser.STORAGE_ID + "\": \"").append(serverId).append("\"\n");
        if (lastEntry) {
            body.append("    }\n");
            body.append("  ]\r\n}");
        } else {
            body.append("    },\n");
        }
        return body.toString();
    }
}
