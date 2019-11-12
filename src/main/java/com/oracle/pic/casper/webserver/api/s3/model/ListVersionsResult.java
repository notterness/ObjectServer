package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.oracle.pic.casper.webserver.api.s3.S3StorageClass;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;

@JsonPropertyOrder({"Name", "Prefix", "KeyMarker", "VersionIdMarker", "nextKeyMarker", "nextVersionIdMarker",
        "MaxKeys", "Delimiter", "IsTruncated", "Encoding-Type", "Version", "CommonPrefixes"})
public class ListVersionsResult extends S3XmlResult {

    private final String name;
    private final String prefix;
    private final String keyMarker;
    private final String versionIdMarker;
    private final String nextKeyMarker;
    private final String nextVersionIdMarker;
    private final int maxKeys;
    private final String delimiter;
    private final boolean isTruncated;
    private final List<ListVersionsResult.Version> version;
    private final String encodingType;
    private final List<Prefix> commonPrefixes;

    @JsonPropertyOrder({"Key", "VersionId", "IsLatest", "LastModified", "ETag", "Size", "StorageClass", "Owner"})
    public static final class Version {
        private final String key;
        private final String versionId;
        private final boolean isLatest;
        private final Date lastModified;
        private final String eTag;
        private final long size;
        private final S3StorageClass storageClass;
        private final Owner owner;

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
        public Version(String key, Date lastModified, String eTag, long size, S3StorageClass storageClass,
                       Owner owner,  @Nullable String versionId) {

            this.key = key;
            this.versionId = (versionId == null) ? generateVersionIdFromKey(key) : versionId;
            this.isLatest = true;
            this.lastModified = lastModified;
            this.eTag = eTag;
            this.size = size;
            this.storageClass = storageClass;
            this.owner = owner;
        }

        public String getKey() {
            return key;
        }

        @JsonProperty("VersionId")
        public String getVersionId() {
            return versionId;
        }

        @JsonProperty("IsLatest")
        public boolean getIsLatest() {
            return isLatest;
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

        public S3StorageClass getStorageClass() {
            return storageClass;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Owner getOwner() {
            return owner;
        }

        static String generateVersionIdFromKey(String key) {
            return "versionId" + key;
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

    public static class Builder {
        private String name;
        private String prefix;
        private String keyMarker;
        private String versionIdMarker;
        private String nextKeyMarker;
        private String nextVersionIdMarker;
        private int maxKeys;
        private String delimiter;
        private boolean isTruncated;
        private String encodingType;
        private List<Prefix> commonPrefixes;
        private List<ListVersionsResult.Version> version;

        public ListVersionsResult.Builder name(String val) {
            this.name = val;
            return this;
        }

        public ListVersionsResult.Builder prefix(String val) {
            this.prefix = val;
            return this;
        }

        public ListVersionsResult.Builder keyMarker(String val) {
            this.keyMarker = val;
            return this;
        }

        public ListVersionsResult.Builder versionIdMarker(String val) {
            this.versionIdMarker = val;
            return this;
        }

        public ListVersionsResult.Builder nextKeyMarker(String val) {
            this.nextKeyMarker = val;
            return this;
        }

        public ListVersionsResult.Builder nextVersionIdMarker(String val) {
            this.nextVersionIdMarker = val;
            return this;
        }

        public ListVersionsResult.Builder maxKeys(int val) {
            this.maxKeys = val;
            return this;
        }

        public ListVersionsResult.Builder delimiter(String val) {
            this.delimiter = val;
            return this;
        }

        public ListVersionsResult.Builder isTruncated(boolean val) {
            isTruncated = val;
            return this;
        }

        public ListVersionsResult.Builder encodingType(String val) {
            this.encodingType = val;
            return this;
        }

        public ListVersionsResult.Builder commonPrefixes(List<Prefix> val) {
            this.commonPrefixes = val;
            return this;
        }

        public ListVersionsResult.Builder versions(List<ListVersionsResult.Version> val) {
            this.version = val;
            return this;
        }

        public String getKeyMarker() {
            return this.keyMarker;
        }

        public ListVersionsResult build() {
            return new ListVersionsResult(this);
        }
    }

    public ListVersionsResult(ListVersionsResult.Builder builder) {
        this.name = builder.name;
        this.prefix = builder.prefix;
        this.keyMarker = builder.keyMarker;
        this.versionIdMarker = builder.versionIdMarker;
        this.nextKeyMarker = builder.nextKeyMarker;
        this.nextVersionIdMarker = builder.nextVersionIdMarker;
        this.maxKeys = builder.maxKeys;
        this.delimiter = builder.delimiter;
        this.isTruncated = builder.isTruncated;
        this.encodingType = builder.encodingType;
        this.commonPrefixes = builder.commonPrefixes;
        this.version = builder.version;
    }

    public String getName() {
        return name;
    }

    public String getPrefix() {
        return prefix;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getKeyMarker() {
        return keyMarker;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getVersionIdMarker() {
        return versionIdMarker;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getNextKeyMarker() {
        return nextKeyMarker;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getNextVersionIdMarker() {
        return nextVersionIdMarker;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDelimiter() {
        return delimiter;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public boolean getIsTruncated() {
        return isTruncated;
    }

    // The S3 documentation says this is property "Encoding-Type" but S3
    // actually returns "<EncodingType>url</EncodingType>"
    @JsonProperty("EncodingType")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getEncodingType() {
        return encodingType;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Iterable<ListVersionsResult.Version> getVersion() {
        return version;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Iterable<Prefix> getCommonPrefixes() {
        return commonPrefixes;
    }

}
