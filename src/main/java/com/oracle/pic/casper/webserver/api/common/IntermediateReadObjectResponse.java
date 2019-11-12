package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.webserver.api.model.WSStorageObject;

import javax.annotation.Nullable;
import java.util.List;

public final class IntermediateReadObjectResponse {

    private final WSStorageObject so;
    private final List<WSStorageObject> parts;
    private final boolean notModified;

    /**
     * @param so metadata for the object, or for the manifest object of a DLO / SLO
     * @param parts list of parts for a DLO / SLO, or null for an ordinary get
     * @param notModified pass true if the requested resource has not been modified according to the request's
     *                    conditional headers (HTTP 304 NOT_MODIFIED)
     */
    public IntermediateReadObjectResponse(WSStorageObject so,
                                          @Nullable List<WSStorageObject> parts,
                                          boolean notModified) {
        this.so = so;
        this.parts = parts;
        this.notModified = notModified;
    }

    public WSStorageObject getStorageObject() {
        return so;
    }

    public List<WSStorageObject> getParts() {
        return parts;
    }

    public boolean resourceNotModified() {
        return notModified;
    }

    public boolean isLargeObject() {
        return parts != null;
    }

    public long getSize() {
        if (parts != null) {
            return parts.stream().mapToLong(WSStorageObject::getTotalSizeInBytes).sum();
        } else {
            return so.getTotalSizeInBytes();
        }
    }
}
