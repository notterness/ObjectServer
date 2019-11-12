package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents result of a LIST query for serialization for Swift
 */
public class SwiftObjectSummaryCollection {

    @JsonProperty("nextStartWith")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String nextStartWith;

    @JsonIgnore
    private final String secondLastObjectInQuery;

    @JsonIgnore
    private final String lastObjectInQuery;

    @JsonIgnore
    private final ObjectSummary nextObject;

    @JsonProperty("objects")
    private final Iterable<ObjectSummary> objects;

    @JsonIgnore
    private final String bucketName;

    @JsonIgnore
    private final Map<String, String> metadata;

    @JsonProperty("prefixes")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Iterable<String> prefixes;

    public SwiftObjectSummaryCollection(
        Iterable<ObjectSummary> objects,
        String bucketName,
        Map<String, String> metadata,
        @Nullable Iterable<String> prefixes,
        @Nullable ObjectSummary nextObject,
        @Nullable String secondLastObjectInQuery,
        @Nullable String lastObjectInQuery) {
        this.objects = objects;
        this.bucketName = bucketName;
        this.metadata = metadata;
        this.nextObject = nextObject;
        this.nextStartWith = nextObject == null ? null : nextObject.getName();
        this.prefixes = prefixes;
        this.secondLastObjectInQuery = secondLastObjectInQuery;
        this.lastObjectInQuery = lastObjectInQuery;
    }

    @Nullable
    public String getNextStartWith() {
        return nextStartWith;
    }

    @Nullable
    @JsonIgnore
    public String getSecondLastObjectInQuery() {
        return secondLastObjectInQuery;
    }

    @Nullable
    @JsonIgnore
    public String getLastObjectInQuery() {
        return lastObjectInQuery;
    }

    @Nullable
    public ObjectSummary getNextObject() {
        return nextObject;
    }

    @Nullable
    public Iterable<String> getPrefixes() {
        return prefixes;
    }

    public Iterable<ObjectSummary> getObjects() {
        return objects;
    }

    public String getBucketName() {
        return bucketName;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}

