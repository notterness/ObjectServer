package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.annotation.Nullable;
import java.util.Date;

@JsonDeserialize(builder = ParUploadMetadata.Builder.class)
public class ParUploadMetadata extends UploadMetadata {

    // the URI that is used by the PAR user for uploading parts
    @JsonProperty("accessUri")
    private final String accessUri;

    public ParUploadMetadata(String namespace, String bucketName, String objectName, String uploadId,
                             @Nullable Date timeCreated, String accessUri) {
        super(namespace, bucketName, objectName, uploadId, timeCreated);
        this.accessUri = accessUri;
    }

    public String getAccessUri() {
        return accessUri;
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
        @JsonProperty("timeCreated")
        private Date timeCreated;
        @JsonProperty("accessUri")
        private String accessUri;

        public Builder namespaceName(String val) {
            namespaceName = val;
            return this;
        }

        public Builder bucketName(String val) {
            bucketName = val;
            return this;
        }

        public Builder objectName(String val) {
            objectName = val;
            return this;
        }

        public Builder uploadId(String val) {
            uploadId = val;
            return this;
        }

        public Builder timeCreated(Date val) {
            this.timeCreated = new Date(val.getTime());
            return this;
        }

        public Builder accessUri(String val) {
            this.accessUri = val;
            return this;
        }

        public ParUploadMetadata build() {
            return new ParUploadMetadata(namespaceName, bucketName, objectName, uploadId, timeCreated, accessUri);
        }
    }
}
