package com.oracle.pic.casper.webserver.api.model;

import java.util.Objects;

/**
 * This class is used as a part of {@link com.oracle.pic.casper.webserver.util.WSRequestContext} to get the logging status on a bucket
 *
 * We deserialize the readLogId and the writeLogId from the bucketOptions and create this POJO to add it to the webserver
 * context.
 *
 * An empty readLogId would mean that we will not log READ or LIST type operations on a bucket.
 * An empty writeLogId would mean that we will not log CREATE, DELETE, UPDATE operations on a bucket.
 *
 * List of all supported operations by Casper - {@link com.oracle.pic.casper.webserver.api.auth.CasperOperation}
 */
public class BucketLoggingStatus {

    private String readLogId;

    private String writeLogId;

    public void setReadLogId(String readLogId) {
        this.readLogId = readLogId;
    }

    public void setWriteLogId(String writeLogId) {
        this.writeLogId = writeLogId;
    }

    public String getReadLogId() {
        return readLogId;
    }

    public String getWriteLogId() {
        return writeLogId;
    }

    @Override
    public String toString() {
        return "serviceLog{" +
                "read='" + readLogId + '\'' +
                ", write='" + writeLogId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BucketLoggingStatus that = (BucketLoggingStatus) o;
        return Objects.equals(readLogId, that.readLogId) &&
                Objects.equals(writeLogId, that.writeLogId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readLogId, writeLogId);
    }


    public static final class BucketLoggingStatusBuilder {
        private String readLogId;
        private String writeLogId;

        public BucketLoggingStatusBuilder() {
        }

        public BucketLoggingStatusBuilder readLogId(String readLogId) {
            this.readLogId = readLogId;
            return this;
        }

        public BucketLoggingStatusBuilder writeLogId(String writeLogId) {
            this.writeLogId = writeLogId;
            return this;
        }

        public BucketLoggingStatus build() {
            BucketLoggingStatus bucketLoggingStatus = new BucketLoggingStatus();
            bucketLoggingStatus.setReadLogId(readLogId);
            bucketLoggingStatus.setWriteLogId(writeLogId);
            return bucketLoggingStatus;
        }
    }
}
