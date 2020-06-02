package com.webutils.webserver.http;

import com.webutils.webserver.requestcontext.ServerIdentifier;

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
        body.append("  \"chunk-" + index + "\":\n");
        body.append("    {\n");
        body.append("       \"chunk-id\": \"" + id + "\"\n");
        body.append("       \"chunk-uid\": \"" + uid + "\"\n");
        body.append("       \"chunk-lba\": \"" + lba + "\"\n");
        body.append("       \"chunk-size\": \"" + size + "\"\n");
        body.append("       \"chunk-state\": \"" + state.toString() + "\"\n");
        body.append("       \"last-allocated\": \"" + lastAllocateTime + "\"\n");
        body.append("       \"last-deleted\": \"" + lastDeleteTime + "\"\n");
        body.append("       \"storage-id\": \"" + serverId + "\"\n");
        if (lastEntry) {
            body.append("    }\n");
            body.append("  ]\r\n}");
        } else {
            body.append("    },\n");
        }
        return body.toString();
    }
}
