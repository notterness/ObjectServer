package com.oracle.pic.casper.webserver.api.s3.model;

import java.time.Instant;
import java.util.Map;

public class CopyObjectDetails {
    private final String sourceBucketName;
    private final String sourceKey;
    private final String destinationBucketName;
    private final String destinationKey;
    private final String ifMatchEtag;
    private final String ifNoneMatchEtag;
    private final Instant unmodifiedSinceConstraint;
    private final Instant modifiedSinceConstraint;
    private final Map<String, String> metadata;

    public CopyObjectDetails(String sourceBucketName,
                             String sourceKey,
                             String destinationBucketName,
                             String destinationKey,
                             String ifMatchEtag,
                             String ifNoneMatchEtag,
                             Instant unmodifiedSinceConstraint,
                             Instant modifiedSinceConstraint,
                             Map<String, String> metadata) {
        this.sourceBucketName = sourceBucketName;
        this.sourceKey = sourceKey;
        this.destinationBucketName = destinationBucketName;
        this.destinationKey = destinationKey;
        this.ifMatchEtag = ifMatchEtag;
        this.ifNoneMatchEtag = ifNoneMatchEtag;
        this.unmodifiedSinceConstraint = unmodifiedSinceConstraint;
        this.modifiedSinceConstraint = modifiedSinceConstraint;
        this.metadata = metadata;
    }

    public String getSourceBucketName() {
        return sourceBucketName;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public String getDestinationBucketName() {
        return destinationBucketName;
    }

    public String getDestinationKey() {
        return destinationKey;
    }

    public String getIfMatchEtag() {
        return ifMatchEtag;
    }

    public String getIfNoneMatchEtag() {
        return ifNoneMatchEtag;
    }

    public Instant getUnmodifiedSinceConstraint() {
        return unmodifiedSinceConstraint;
    }

    public Instant getModifiedSinceConstraint() {
        return modifiedSinceConstraint;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}
