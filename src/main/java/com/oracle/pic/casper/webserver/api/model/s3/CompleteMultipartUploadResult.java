package com.oracle.pic.casper.webserver.api.model.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;

public class CompleteMultipartUploadResult extends S3XmlResult {
    private final String location;
    private final String bucket;
    private final String key;
    private final String uploadId;
    private final String eTag;

    public CompleteMultipartUploadResult(String location, String bucket, String key, String uploadId, String eTag) {
        this.location = location;
        this.bucket = bucket;
        this.key = key;
        this.uploadId = uploadId;
        this.eTag = eTag;
    }

    public String getLocation() {
        return location;
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

    @JsonProperty("ETag")
    public String getETag() {
        return eTag;
    }

    @Override
    public String toString() {
        return "CompleteMultipartUploadResult{" +
            "location='" + location + '\'' +
            ", bucket='" + bucket + '\'' +
            ", key='" + key + '\'' +
            ", uploadId='" + uploadId + '\'' +
            ", eTag='" + eTag + '\'' +
            '}';
    }
}
