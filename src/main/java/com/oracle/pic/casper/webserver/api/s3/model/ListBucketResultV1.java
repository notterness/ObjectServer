package com.oracle.pic.casper.webserver.api.s3.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.webserver.api.s3.S3StorageClass;
import com.oracle.pic.casper.webserver.api.s3.S3XmlResult;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;

@JacksonXmlRootElement(localName = "ListBucketResult")
@JsonPropertyOrder({"Name", "Prefix", "Marker", "NextMarker", "MaxKeys", "Delimiter", "IsTruncated", "EncodingType",
    "Contents", "CommonPrefixes"})
public class ListBucketResultV1 extends S3XmlResult {

    private final String name;
    private final String prefix;
    private final int maxKeys;
    private final String delimiter;
    private final boolean isTruncated;
    private final String encodingType;
    private final List<Prefix> commonPrefixes;
    private final List<Content> contents;
    private final String marker;
    private final String nextMarker;

    @JsonPropertyOrder({"Key", "LastModified", "ETag", "Size", "StorageClass", "Owner"})
    public static final class Content {

        private final String key;
        private final Date lastModified;
        private final String eTag;
        private final long size;
        private final S3StorageClass storageClass;
        private final Owner owner;

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
        public Content(String key, Date lastModified, String eTag, long size, S3StorageClass storageClass,
                       @Nullable Owner owner) {
            this.key = key;
            this.lastModified = lastModified;
            this.eTag = eTag;
            this.size = size;
            this.storageClass = storageClass;
            this.owner = owner;
        }

        public String getKey() {
            return key;
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
        private int maxKeys;
        private String delimiter;
        private boolean isTruncated;
        private String encodingType;
        private List<Prefix> commonPrefixes;
        private List<Content> contents;
        private String marker;
        private String nextMarker;

        public Builder name(String val) {
            this.name = val;
            return this;
        }

        public Builder prefix(String val) {
            this.prefix = val;
            return this;
        }

        public Builder maxKeys(int val) {
            this.maxKeys = val;
            return this;
        }

        public Builder delimiter(String val) {
            this.delimiter = val;
            return this;
        }

        public Builder truncated(boolean val) {
            isTruncated = val;
            return this;
        }

        public Builder encodingType(String val) {
            this.encodingType = val;
            return this;
        }

        public Builder commonPrefixes(List<Prefix> val) {
            this.commonPrefixes = val;
            return this;
        }

        public Builder contents(List<Content> val) {
            this.contents = val;
            return this;
        }

        public Builder marker(String val) {
            this.marker = val;
            return this;
        }

        public Builder nextMarker(String val) {
            this.nextMarker = val;
            return this;
        }

        public ListBucketResultV1 build() {
            return new ListBucketResultV1(this);
        }
    }

    public ListBucketResultV1(Builder builder) {
        this.name = builder.name;
        this.prefix = builder.prefix;
        this.maxKeys = builder.maxKeys;
        this.delimiter = builder.delimiter;
        this.isTruncated = builder.isTruncated;
        this.encodingType = builder.encodingType;
        this.commonPrefixes = builder.commonPrefixes;
        this.contents = builder.contents;
        this.marker = builder.marker;
        this.nextMarker = builder.nextMarker;
    }

    public String getName() {
        return name;
    }

    public String getPrefix() {
        return prefix;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDelimiter() {
        return delimiter;
    }

    public String getMarker() {
        return marker;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getNextMarker() {
        return nextMarker;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public boolean getIsTruncated() {
        return isTruncated;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getEncodingType() {
        return encodingType;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Iterable<Content> getContents() {
        return contents;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Iterable<Prefix> getCommonPrefixes() {
        return commonPrefixes;
    }
}
