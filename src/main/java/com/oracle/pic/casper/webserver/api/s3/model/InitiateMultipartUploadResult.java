package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;

public final class InitiateMultipartUploadResult extends S3XmlResult {
    private final String bucket;
    private final String key;
    private final String uploadId;

    public InitiateMultipartUploadResult(String bucket, String key, String uploadId) {
        this.bucket = bucket;
        this.key = key;
        this.uploadId = uploadId;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getUploadId() {
        return uploadId;
    }

    @JsonIgnore
    @Override
    public String toString() {
        return "InitiateMultipartUploadResult{" +
            "bucket='" + bucket + '\'' +
            ", key='" + key + '\'' +
            ", uploadId='" + uploadId + '\'' +
            '}';
    }
}
