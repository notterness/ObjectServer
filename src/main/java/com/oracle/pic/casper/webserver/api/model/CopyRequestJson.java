package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.HashMap;
import java.util.Map;

/**
 * This is the json object to parse copy object request body.
 * About metadata: if the field is missing in the json, set it to an empty map so merging with old metadata results in
 * the old metadata.  If the field is null, set it to null so that old metadata is removed.
 */
public final class CopyRequestJson {

    private final String sourceObjectName;
    private final String sourceObjectIfMatchETag;
    private final String destinationRegion;
    private final String destinationNamespace;
    private final String destinationBucket;
    private final String destinationObjectName;
    private final String destinationObjectIfMatchETag;
    private final String destinationObjectIfNoneMatchETag;
    //Use default value of empty map if metadata field is missing, which reflects our interpretation of its behavior.
    private Map<String, String> destinationObjectMetadata = new HashMap<>();

    @JsonCreator
    private CopyRequestJson(@JsonProperty("sourceObjectName") String sourceObjectName,
                            @JsonProperty("sourceObjectIfMatchETag") String sourceObjectIfMatchETag,
                            @JsonProperty("destinationRegion") String destinationRegion,
                            @JsonProperty("destinationNamespace") String destinationNamespace,
                            @JsonProperty("destinationBucket") String destinationBucket,
                            @JsonProperty("destinationObjectName") String destinationObjectName,
                            @JsonProperty("destinationObjectIfMatchETag") String destinationObjectIfMatchETag,
                            @JsonProperty("destinationObjectIfNoneMatchETag") String destinationObjectIfNoneMatchETag) {
        this.sourceObjectName = sourceObjectName;
        this.sourceObjectIfMatchETag = sourceObjectIfMatchETag;
        this.destinationRegion = destinationRegion;
        this.destinationNamespace = destinationNamespace;
        this.destinationBucket = destinationBucket;
        this.destinationObjectName = destinationObjectName;
        this.destinationObjectIfMatchETag = destinationObjectIfMatchETag;
        this.destinationObjectIfNoneMatchETag = destinationObjectIfNoneMatchETag;
    }

    public String getSourceObjectName() {
        return sourceObjectName;
    }

    public String getSourceObjectIfMatchETag() {
        return sourceObjectIfMatchETag;
    }

    public String getDestinationRegion() {
        return destinationRegion;
    }

    public String getDestinationNamespace() {
        return destinationNamespace;
    }

    public String getDestinationBucket() {
        return destinationBucket;
    }

    public String getDestinationObjectName() {
        return destinationObjectName;
    }

    public String getDestinationObjectIfMatchETag() {
        return destinationObjectIfMatchETag;
    }

    public String getDestinationObjectIfNoneMatchETag() {
        return destinationObjectIfNoneMatchETag;
    }

    public Map<String, String> getDestinationObjectMetadata() {
        return destinationObjectMetadata;
    }

    public void setDestinationObjectMetadata(Map<String, String> destinationObjectMetadata) {
        this.destinationObjectMetadata = destinationObjectMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CopyRequestJson that = (CopyRequestJson) o;
        return Objects.equal(sourceObjectName, that.sourceObjectName) &&
            Objects.equal(sourceObjectIfMatchETag, that.sourceObjectIfMatchETag) &&
            Objects.equal(destinationRegion, that.destinationRegion) &&
            Objects.equal(destinationNamespace, that.destinationNamespace) &&
            Objects.equal(destinationBucket, that.destinationBucket) &&
            Objects.equal(destinationObjectName, that.destinationObjectName) &&
            Objects.equal(destinationObjectIfMatchETag, that.destinationObjectIfMatchETag) &&
            Objects.equal(destinationObjectIfNoneMatchETag, that.destinationObjectIfNoneMatchETag) &&
            Objects.equal(destinationObjectMetadata, that.destinationObjectMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sourceObjectName, sourceObjectIfMatchETag, destinationRegion, destinationNamespace,
            destinationBucket, destinationObjectName, destinationObjectIfMatchETag, destinationObjectIfNoneMatchETag,
            destinationObjectMetadata);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("sourceObjectName", sourceObjectName)
            .add("sourceObjectIfMatchETag", sourceObjectIfMatchETag)
            .add("destinationRegion", destinationRegion)
            .add("destinationNamespace", destinationNamespace)
            .add("destinationBucket", destinationBucket)
            .add("destinationObjectName", destinationObjectName)
            .add("destinationObjectIfMatchETag", destinationObjectIfMatchETag)
            .add("destinationObjectIfNoneMatchETag", destinationObjectIfNoneMatchETag)
            .add("destinationObjectMetadata", destinationObjectMetadata)
            .toString();
    }
}
