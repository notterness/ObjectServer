package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = UploadPageToken.Builder.class)
public class UploadPageToken {
    @JsonProperty("objectName")
    private final String objectName;

    @JsonProperty("uploadId")
    private final String uploadId;

    public UploadPageToken(String objectName, String uploadId) {
        this.objectName = objectName;
        this.uploadId = uploadId;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getUploadId() {
        return uploadId;
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        @JsonProperty("objectName")
        private String objectName;
        @JsonProperty("uploadId")
        private String uploadId;

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

        public UploadPageToken build() {
            return new UploadPageToken(objectName, uploadId);
        }
    }
}
