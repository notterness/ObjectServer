package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.MoreObjects;

@JsonDeserialize(builder = UploadIdentifier.Builder.class)
public class UploadIdentifier {
    @JsonProperty("namespace")
    private final String namespace;

    @JsonProperty("bucket")
    private final String bucketName;

    @JsonProperty("object")
    private final String objectName;

    /**
     * Unique identifier of an upload.
     */
    @JsonProperty("uploadId")
    private final String uploadId;

    public UploadIdentifier(String namespace, String bucketName, String objectName, String uploadId) {
        this.namespace = namespace;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.uploadId = uploadId;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getUploadId() {
        return uploadId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("namespace", namespace)
            .add("bucketName", bucketName)
            .add("objectName", objectName)
            .add("uploadId", uploadId)
            .addValue(super.toString())
            .toString();
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        @JsonProperty("namespace")
        private String namespaceName;
        @JsonProperty("bucket")
        private String bucketName;
        @JsonProperty("object")
        private String objectName;
        @JsonProperty("uploadId")
        private String uploadId;

        public Builder namespaceName(String val) {
            namespaceName = val;
            return this;
        }

        public String getNamespaceName() {
            return namespaceName;
        }

        public Builder bucketName(String val) {
            bucketName = val;
            return this;
        }

        public String getBucketName() {
            return bucketName;
        }

        public Builder objectName(String val) {
            objectName = val;
            return this;
        }

        public String getObjectName() {
            return objectName;
        }

        public Builder uploadId(String val) {
            uploadId = val;
            return this;
        }

        public String getUploadId() {
            return uploadId;
        }

        public UploadIdentifier build() {
            return new UploadIdentifier(namespaceName, bucketName, objectName, uploadId);
        }
    }
}
