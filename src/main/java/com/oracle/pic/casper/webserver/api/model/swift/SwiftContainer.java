package com.oracle.pic.casper.webserver.api.model.swift;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oracle.pic.casper.webserver.api.model.BucketSummary;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingBucketNameException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used for representing Swift containers (buckets).
 *
 * It can be automatically serialized and deserialized to and from JSON or XML, although we never really use
 * deserializing.
 */
@JsonDeserialize(builder = SwiftContainer.Builder.class)
@JacksonXmlRootElement(localName = "container")
public final class SwiftContainer {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("count")
    private final long count;

    @JsonProperty("bytes")
    private final long bytes;

    @JsonIgnore
    private final Map<String, String> metadata;

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {

        // TODO(jfriedly):  Real Swift supports an object count on a container and a sum of the total number of bytes
        // stored in the container.
        private static final long STATIC_COUNT = 0L;
        private static final long STATIC_BYTES = 0L;

        @JsonProperty("name")
        private String name;
        @JsonProperty("count")
        private long count = STATIC_COUNT;
        @JsonProperty("bytes")
        private long bytes = STATIC_BYTES;
        @JsonIgnore
        private Map<String, String> metadata;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public SwiftContainer build() {
            if (name == null) {
                throw new MissingBucketNameException();
            }

            // TODO(jfriedly):  validate the container name here
            return new SwiftContainer(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Builder copy) {
        Builder builder = new Builder();
        builder.name = copy.name;
        builder.count = copy.count;
        builder.bytes = copy.bytes;
        builder.metadata = copy.metadata;
        return builder;
    }

    private SwiftContainer(Builder builder) {
        this.name = builder.name;
        this.count = builder.count;
        this.bytes = builder.bytes;
        this.metadata = builder.metadata;
    }

    public static SwiftContainer fromBucketSummary(BucketSummary bucketSummary) {
        return new Builder()
                .setName(bucketSummary.getBucketName())
                .build();
    }

    public static SwiftContainer fromBucket(
        String bucketName, Map<String, String> metadata) {
        return new Builder().setName(bucketName).setMetadata(metadata).build();
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public long getCount() {
        return count;
    }

    public long getBytes() {
        return bytes;
    }

    @Nonnull
    public Map<String, String> getMetadata() {
        return new HashMap<>(metadata);
    }
}
