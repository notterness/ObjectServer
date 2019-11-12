package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.objectmeta.ChecksumType;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;

import javax.annotation.Nullable;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class ObjectS3Summary implements ObjectBaseSummary {

    private final String objectName;
    private final Instant modificationTime;
    private final long totalSizeInBytes;
    private final String md5;
    private final ChecksumType checksumType;
    private final int partCount;
    private final ArchivalState archivalState;



    public ObjectS3Summary(String objectName,
                           Instant modificationTime,
                           long totalSizeInBytes,
                           @Nullable String md5,
                           ChecksumType checksumType,
                           int partCount,
                           ArchivalState archivalState) {
        this.objectName = Preconditions.checkNotNull(objectName);
        this.modificationTime = Preconditions.checkNotNull(modificationTime);
        this.totalSizeInBytes = totalSizeInBytes;
        this.md5 = md5;
        this.checksumType = Preconditions.checkNotNull(checksumType);
        this.partCount = partCount;
        this.archivalState = Preconditions.checkNotNull(archivalState);
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
                case TIMEMODIFIED:
                    objectSummary.setTimeModified(Date.from(modificationTime));
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
                case ARCHIVALSTATE:
                    objectSummary.setArchivalState(archivalState);
                    break;
                case ARCHIVED:
                    // There is a bug where we are still requesting the deprecated ARCHIVED property. We currently
                    // don't do anything when this property is requested. CASPER-6472
                    break;
                default:
                    // We pre-validate the object properties for S3 lists, so if we don't recognize a property,
                    // fail fast because something is wrong.
                    throw new IllegalArgumentException(
                            "Object Property '" + op + "' not valid for ObjectS3Summary");
            }
        }
        return objectSummary;
    }

    public Instant getModificationTime() {
        return modificationTime;
    }

    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public String getMd5() {
        return md5;
    }

    public ChecksumType getChecksumType() {
        return checksumType;
    }

    public int getPartCount() {
        return partCount;
    }

    public ArchivalState getArchivalState() {
        return archivalState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectS3Summary that = (ObjectS3Summary) o;
        return totalSizeInBytes == that.totalSizeInBytes &&
                partCount == that.partCount &&
                Objects.equals(objectName, that.objectName) &&
                Objects.equals(modificationTime, that.modificationTime) &&
                Objects.equals(md5, that.md5) &&
                checksumType == that.checksumType &&
                archivalState == that.archivalState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectName,
                modificationTime,
                totalSizeInBytes,
                md5,
                checksumType,
                partCount,
                archivalState);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("objectName", objectName)
                .add("modificationTime", modificationTime)
                .add("totalSizeInBytes", totalSizeInBytes)
                .add("md5", md5)
                .add("checksumType", checksumType)
                .add("partCount", partCount)
                .add("archivalState", archivalState)
                .toString();
    }
}
