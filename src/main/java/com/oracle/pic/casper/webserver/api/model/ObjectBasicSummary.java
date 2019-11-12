package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;

import javax.annotation.Nullable;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class ObjectBasicSummary implements ObjectBaseSummary {

    private final String objectName;
    private final Instant creationTime;
    private final long totalSizeInBytes;
    private final String md5;
    private final ChecksumType checksumType;
    private final int partCount;
    private final String etag;

    public ObjectBasicSummary(String objectName,
                              Instant creationTime,
                              long totalSizeInBytes,
                              @Nullable String md5,
                              ChecksumType checksumType,
                              int partCount,
                              String etag) {
        this.objectName = Preconditions.checkNotNull(objectName);
        this.creationTime = Preconditions.checkNotNull(creationTime);
        this.totalSizeInBytes = totalSizeInBytes;
        this.md5 = md5;
        this.checksumType = Preconditions.checkNotNull(checksumType);
        this.partCount = partCount;
        this.etag = Preconditions.checkNotNull(etag);
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
                case NAME:
                    objectSummary.setName(objectName);
                    break;
                case TIMECREATED:
                    objectSummary.setTimeCreated(Date.from(creationTime));
                    break;
                case SIZE:
                    objectSummary.setSize(totalSizeInBytes);
                    break;
                case MD5:
                    Checksum checksum = checksumType == ChecksumType.MD5 ?
                            Checksum.fromBase64(md5) :
                            Checksum.fromMultipartBase64(md5,
                                    partCount);
                    objectSummary.setChecksum(checksum);
                    break;
                case ETAG:
                    objectSummary.setETag(etag);
                    break;
                default:
                    // We pre-validate the object properties for basic lists, so if we don't recognize a property,
                    // fail fast because something is wrong.
                    throw new IllegalArgumentException(
                            "Object Property '" + op + "' not valid for ObjectBasicSummary");
            }
        }
        return objectSummary;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public String getMd5() {
        return md5;
    }

    public String getEtag() {
        return etag;
    }

    public ChecksumType getChecksumType() {
        return checksumType;
    }

    public int getPartCount() {
        return partCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectBasicSummary that = (ObjectBasicSummary) o;
        return totalSizeInBytes == that.totalSizeInBytes &&
                partCount == that.partCount &&
                Objects.equals(objectName, that.objectName) &&
                Objects.equals(creationTime, that.creationTime) &&
                Objects.equals(md5, that.md5) &&
                checksumType == that.checksumType &&
                Objects.equals(etag, that.etag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectName,
                creationTime,
                totalSizeInBytes,
                md5,
                etag,
                checksumType,
                partCount);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("objectName", objectName)
                .add("creationTime", creationTime)
                .add("totalSizeInBytes", totalSizeInBytes)
                .add("md5", md5)
                .add("etag", etag)
                .add("checksumType", checksumType)
                .add("partCount", partCount)
                .toString();
    }
}
