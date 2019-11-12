package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;

import javax.annotation.Nullable;

/**
 * Represents result of a LIST query for serialization
 */
public class ObjectSummaryCollection {

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

    @JsonProperty("prefixes")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Iterable<String> prefixes;

    public ObjectSummaryCollection(Iterable<ObjectSummary> objects,
                                   String bucketName,
                                   @Nullable Iterable<String> prefixes,
                                   @Nullable ObjectSummary nextObject,
                                   @Nullable String secondLastObjectInQuery,
                                   @Nullable String lastObjectInQuery) {
        this.objects = objects;
        this.bucketName = bucketName;
        this.nextObject = nextObject;
        this.nextStartWith = nextObject == null ? null : nextObject.getName();
        this.prefixes = prefixes;
        this.secondLastObjectInQuery = secondLastObjectInQuery;
        this.lastObjectInQuery = lastObjectInQuery;
    }

    public ObjectSummaryCollection(Iterable<ObjectSummary> objects,
                                   String bucketName,
                                   @Nullable Iterable<String> prefixes,
                                   @Nullable String nextStartWith,
                                   @Nullable String secondLastObjectInQuery,
                                   @Nullable String lastObjectInQuery) {
        this.objects = objects;
        this.bucketName = bucketName;
        this.nextStartWith = nextStartWith;
        this.nextObject = null;
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
}
