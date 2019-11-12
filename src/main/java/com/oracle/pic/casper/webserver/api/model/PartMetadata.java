package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Date;

/**
 * Contains information about a part that is exposed externally.
 * A part is a chunk of data that belongs to an upload of a multipart object.
 */
public class PartMetadata {
    /**
     * Unique identifier for a part that is declared externally.
     */
    private final int partNumber;

    /**
     * Internally created tag that uniquely identifies the part.
     */
    private final String etag;
    private final String md5;
    private final long size;
    private final Date lastModified;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public PartMetadata(int partNumber, String etag, String md5, long size, Date lastModified) {
        this.partNumber = partNumber;
        this.etag = etag;
        this.md5 = md5;
        this.size = size;
        this.lastModified = lastModified;
    }

    public int getPartNumber() {
        return partNumber;
    }

    public String partNumberAsString() {
        return String.valueOf(partNumber);
    }

    public String getEtag() {
        return etag;
    }

    public String getMd5() {
        return md5;
    }

    public long getSize() {
        return size;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public Date getLastModified() {
        return lastModified;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("partNumber", partNumber)
                .add("etag", etag)
                .add("md5", md5)
                .add("size", size)
                .add("lastModified", lastModified)
                .toString();
    }
}
