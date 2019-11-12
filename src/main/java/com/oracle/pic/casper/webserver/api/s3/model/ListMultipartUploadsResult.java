package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.oracle.pic.casper.webserver.api.model.UploadMetadata;
import com.oracle.pic.casper.webserver.api.s3.S3StorageClass;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;
import com.oracle.pic.identity.authentication.Principal;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

@JsonPropertyOrder({"Bucket", "KeyMarker", "UploadIdMarker", "NextKeyMarker", "NextUploadIdMarker", "Delimiter",
        "Prefix", "MaxUploads", "IsTruncated", "Upload", "CommonPrefixes", "Encoding-Type"})
public final class ListMultipartUploadsResult extends S3XmlResult {
    private final String bucket;
    private final String keyMarker;
    private final String uploadIdMarker;
    private final String nextKeyMarker;
    private final String nextUploadIdMarker;
    private final String encodingType;
    private final int maxUploads;
    private final String delimiter;
    private final String prefix;
    private List<Prefix> commonPrefixes;
    private final boolean isTruncated;
    private List<Upload> upload;


    public ListMultipartUploadsResult(String bucket, @Nullable String keyMarker, @Nullable String uploadIdMarker,
                                      @Nullable String nextKeyMarker, @Nullable String nextUploadIdMarker,
                                      @Nullable String encodingType, int maxUploads, @Nullable String delimiter,
                                      @Nullable String prefix, @Nullable List<Prefix> commonPrefixes,
                                      boolean isTruncated, List<Upload> uploads, Principal principal) {
        this.bucket = bucket;
        this.keyMarker = keyMarker;
        this.uploadIdMarker = uploadIdMarker;
        this.nextKeyMarker = nextKeyMarker;
        this.nextUploadIdMarker = nextUploadIdMarker;
        this.encodingType = encodingType;
        this.maxUploads = maxUploads;
        this.delimiter = delimiter;
        this.prefix = prefix;
        this.commonPrefixes = commonPrefixes;
        this.isTruncated = isTruncated;
        this.upload = uploads;
    }

    @JsonPropertyOrder({"Key", "UploadId", "Initiator", "Owner", "StorageClass", "Initiated"})
    public static final class Upload {
        private final String key;
        private final String uploadId;
        private final S3StorageClass storageClass;
        private final Owner owner;
        private final Date initiated;

        public Upload(UploadMetadata uploadMetadata, boolean encode, Principal principal) {
            this(uploadMetadata.getObjectName(), uploadMetadata.getUploadId(), S3StorageClass.STANDARD, encode,
                principal, uploadMetadata.getTimeCreated());
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
        public Upload(String key, String uploadId, S3StorageClass storageClass, boolean encode,
               Principal principal, Date initiated) {
            this.key = encode ? encode(key) : key;
            this.uploadId = uploadId;
            this.storageClass = storageClass;
            this.owner = new Owner(principal);
            this.initiated = initiated;
        }

        public String getKey() {
            return key;
        }

        public String getUploadId() {
            return uploadId;
        }

        public String getStorageClass() {
            return storageClass.toString();
        }

        public Owner getOwner() {
            return owner;
        }

        public Owner getInitiator() {
            return owner;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP")
        public Date getInitiated() {
            return initiated;
        }
    }

    public static final class Prefix {

        private final String prefix;

        public Prefix(String prefix) {
            this.prefix = prefix;
        }

        public String getPrefix() {
            return prefix;
        }
    }

    public String getBucket() {
        return bucket;
    }

    public String getKeyMarker() {
        return keyMarker;
    }

    public String getUploadIdMarker() {
        return uploadIdMarker;
    }

    public String getNextKeyMarker() {
        return nextKeyMarker;
    }

    public String getNextUploadIdMarker() {
        return nextUploadIdMarker;
    }

    @JsonProperty("Encoding-Type")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getEncodingType() {
        return encodingType;
    }

    public int getMaxUploads() {
        return maxUploads;
    }

    public boolean getIsTruncated() {
        return isTruncated;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Iterable<Upload> getUpload() {
        return upload;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getDelimiter() {
        return delimiter;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getPrefix() {
        return prefix;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Iterable<Prefix> getCommonPrefixes() {
        return commonPrefixes;
    }

    protected static String encode(String unencoded) {
        try {
            return unencoded != null ? URLEncoder.encode(unencoded, UTF_8.name()) : unencoded;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonIgnore
    @Override
    public String toString() {
        return "ListMultipartUploadsResult{" +
                "bucket='" + bucket + '\'' +
                ", keyMarker='" + keyMarker + '\'' +
                ", uploadIdMarker='" + uploadIdMarker + '\'' +
                ", nextKeyMarker='" + nextKeyMarker + '\'' +
                ", nextUploadIdMarker='" + nextUploadIdMarker + '\'' +
                ", encodingType='" + encodingType + '\'' +
                ", maxUploads=" + maxUploads +
                ", delimiter='" + delimiter + '\'' +
                ", prefix='" + prefix + '\'' +
                ", commonPrefixes=" + commonPrefixes +
                ", isTruncated=" + isTruncated +
                ", upload=" + upload +
                '}';
    }
}
