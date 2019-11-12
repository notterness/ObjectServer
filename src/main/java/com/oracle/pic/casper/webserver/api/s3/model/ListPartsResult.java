package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.oracle.pic.casper.webserver.api.common.Checksum;
import com.oracle.pic.casper.webserver.api.model.PartMetadata;
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

@JsonPropertyOrder({"Bucket", "Key", "UploadId", "Initiator", "Owner", "StorageClass", "PartNumberMarker",
"NextPartNumberMarker", "MaxParts", "Encoding-Type", "IsTruncated", "Part"})
public final class ListPartsResult extends S3XmlResult {
    private final String bucket;
    private final String encodingType;
    private final String key;
    private final String uploadId;
    private final Integer partNumberMarker;
    private final Integer nextPartNumberMarker;
    private final int maxParts;
    private final boolean isTruncated;
    private final S3StorageClass storageClass;
    private final Owner owner;

    private final List<Part> part;

    public ListPartsResult(String bucket, @Nullable String encodingType, String key, String uploadId,
                           @Nullable Integer partNumberMarker, @Nullable Integer nextPartNumberMarker, int maxParts,
                           boolean isTruncated, List<Part> parts, S3StorageClass storageClass,
                           Principal principal) {
        this.bucket = bucket;
        this.encodingType = encodingType;
        this.key = key;
        this.uploadId = uploadId;
        this.partNumberMarker = partNumberMarker == null ? Integer.valueOf(0) : partNumberMarker;
        this.nextPartNumberMarker = nextPartNumberMarker;
        this.maxParts = maxParts;
        this.isTruncated = isTruncated;
        this.part = parts;
        this.storageClass = storageClass;
        this.owner = new Owner(principal);
    }

    @JsonPropertyOrder({"PartNumber", "LastModified", "ETag", "Size"})
    public static final class Part {
        private final int partNumber;
        private final Date lastModified;
        private final String eTag;
        private final long size;

        public Part(PartMetadata partMetadata) {
            partNumber = partMetadata.getPartNumber();
            lastModified = partMetadata.getLastModified();
            eTag = Checksum.fromBase64(partMetadata.getMd5()).getQuotedHex();
            size = partMetadata.getSize();
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
        public Part(int partNumber, Date lastModified, String eTag, long size) {
            this.partNumber = partNumber;
            this.lastModified = lastModified;
            this.eTag = eTag;
            this.size = size;
        }

        public int getPartNumber() {
            return partNumber;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP")
        public Date getLastModified() {
            return lastModified;
        }

        @JsonProperty("ETag")
        public String getETag() {
            return eTag;
        }

        public long getSize() {
            return size;
        }
    }

    public String getBucket() {
        return bucket;
    }

    @JsonProperty("Encoding-Type")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getEncodingType() {
        return encodingType;
    }

    public String getKey() {
        return encodingType == null ? key : encode(key);
    }

    public String getUploadId() {
        return uploadId;
    }

    public Integer getPartNumberMarker() {
        return partNumberMarker;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNextPartNumberMarker() {
        return nextPartNumberMarker;
    }

    public int getMaxParts() {
        return maxParts;
    }

    public boolean getIsTruncated() {
        return isTruncated;
    }

    @JacksonXmlElementWrapper(useWrapping = false)
    public Iterable<Part> getPart() {
        return part;
    }

    public S3StorageClass getStorageClass() {
        return storageClass;
    }

    public Owner getOwner() {
        return owner;
    }

    public Owner getInitiator() {
        return owner;
    }

    protected static String encode(String unencoded) {
        try {
            return unencoded != null ? URLEncoder.encode(unencoded, UTF_8.name()) : unencoded;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
