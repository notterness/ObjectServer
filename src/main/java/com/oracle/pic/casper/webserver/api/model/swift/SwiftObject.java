package com.oracle.pic.casper.webserver.api.model.swift;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.util.DateUtil;
import com.oracle.pic.casper.webserver.api.common.HttpHeaderHelpers;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;
import io.vertx.core.http.HttpHeaders;

import java.util.Date;
import java.util.regex.Pattern;

/**
 * This class is used for representing Swift objects.
 *
 * It can be automatically serialized and deserialized to and from JSON or XML, although we never really use
 * deserializing.
 */
@JsonDeserialize(builder = SwiftObject.Builder.class)
@JacksonXmlRootElement(localName = "object")
public final class SwiftObject {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("hash")
    private final String hexMD5Hash;

    @JsonProperty("bytes")
    private final long bytes;

    // TODO(jfriedly):  verify this dateformat pattern
    @JsonProperty("last_modified")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DateUtil.SWIFT_DATE_TIME_PATTERN)
    private final Date lastModified;

    @JsonProperty("content_type")
    private final String contentType;

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {

        private static final Pattern HEX = Pattern.compile("^[0-9a-f]{32}$");

        @JsonProperty("name")
        private String name;
        @JsonProperty("hash")
        private String hexMD5Hash;
        @JsonProperty("bytes")
        private long bytes;
        @JsonProperty("last_modified")
        private Date lastModified;
        @JsonProperty("content_type")
        private String contentType;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the hash to the hex MD5.  Do *not* call this method with a base64-encoded MD5.
         */
        public Builder setHexMD5Hash(String hexMD5Hash) {
            this.hexMD5Hash = hexMD5Hash;
            return this;
        }

        public Builder setBytes(long bytes) {
            this.bytes = bytes;
            return this;
        }

        public Builder setContentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder setLastModified(Date lastModified) {
            this.lastModified = new Date(lastModified.getTime());
            return this;
        }

        public SwiftObject build() {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(hexMD5Hash);
            Preconditions.checkArgument(bytes >= 0);
            Preconditions.checkNotNull(lastModified);
            return new SwiftObject(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Builder copy) {
        Builder builder = new Builder();
        builder.name = copy.name;
        builder.hexMD5Hash = copy.hexMD5Hash;
        builder.bytes = copy.bytes;
        builder.lastModified = copy.lastModified;
        builder.contentType = copy.contentType;
        return builder;
    }

    private SwiftObject(Builder builder) {
        this.name = builder.name;
        this.hexMD5Hash = builder.hexMD5Hash;
        this.bytes = builder.bytes;
        this.lastModified = builder.lastModified;
        this.contentType = builder.contentType;
    }

    /**
     * Convert an ObjectSummary to a serializable SwiftObject, translating the MD5 from base64 to hex.
     */
    public static SwiftObject fromObjectSummary(ObjectSummary objectSummary) {
        Preconditions.checkNotNull(objectSummary.getName());
        Preconditions.checkNotNull(objectSummary.getChecksum());
        Preconditions.checkNotNull(objectSummary.getSize());
        Preconditions.checkNotNull(objectSummary.getTimeModified());

        final Builder builder = new Builder()
            .setName(objectSummary.getName())
            .setHexMD5Hash(objectSummary.getChecksum().getHex())
            .setBytes(objectSummary.getSize())
            .setLastModified(objectSummary.getTimeModified());

        if (objectSummary.getMetadata() != null) {
            // If we have object metadata we will use it to return the content-type
            // of of the object. If there is no metadata we leave content-type
            // as null (CASPER-6145).
            String contentTypeKey = HttpHeaderHelpers.PRESERVED_CONTENT_HEADERS.get(
                HttpHeaders.CONTENT_TYPE.toString().toLowerCase());
            String contentType =
                objectSummary.getMetadata().getOrDefault(contentTypeKey, ContentType.APPLICATION_OCTET_STREAM);
            builder.setContentType(contentType);
        }

        return builder.build();
    }

    public String getName() {
        return name;
    }

    /**
     * Returns the MD5 hash of the object as a 32-character hex string
     *
     * Note that the backend returns base64-encoded strings, so they must be converted before building SwiftObjects.
     */
    public String getHexMD5Hash() {
        return hexMD5Hash;
    }

    public long getBytes() {
        return bytes;
    }

    public Date getLastModified() {
        return new Date(lastModified.getTime());
    }

    public String getContentType() {
        return contentType;
    }
}
