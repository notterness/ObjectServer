package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.objectmeta.UploadInfo;

import javax.annotation.Nullable;
import java.util.Date;

/**
 * An upload is an in-progress write to an object. An object may have multiple open uploads.
 * The upload is reflected on the object once it is committed. The upload is removed once it is
 * committed.
 */
@JsonDeserialize(builder = UploadMetadata.Builder.class)
public class UploadMetadata extends UploadIdentifier {

    @Nullable
    @JsonProperty("timeCreated")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DateUtil.SWAGGER_DATE_TIME_PATTERN)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Date timeCreated;


    public UploadMetadata(String namespace, String bucketName, String objectName, String uploadId,
                          @Nullable Date timeCreated) {
        super(namespace, bucketName, objectName, uploadId);
        this.timeCreated = timeCreated == null ? null : new Date(timeCreated.getTime());
    }

    @Nullable
    public Date getTimeCreated() {
        return timeCreated == null ? null : new Date(timeCreated.getTime());
    }

    public UploadPageToken toPage() {
        return new UploadPageToken(getObjectName(), getUploadId());
    }

    @Nullable
    public static UploadMetadata toUploadMetadata(UploadInfo uploadInfo) {
        UploadMetadata uploadMetadata = new UploadMetadata(
                uploadInfo.getUploadKey().getObjectKey().getBucket().getNamespace(),
                uploadInfo.getUploadKey().getObjectKey().getBucket().getName(),
                uploadInfo.getUploadKey().getObjectKey().getName(),
                uploadInfo.getUploadKey().getUploadId(),
                Date.from(uploadInfo.getCreationTime()));
        return uploadMetadata;
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

        public UploadMetadata build() {
            return new UploadMetadata(namespaceName, bucketName, objectName, uploadId, timeCreated);
        }
    }
}
