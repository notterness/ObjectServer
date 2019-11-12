package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;

import java.util.Map;

/**
 * Request payload for multipart upload creation.
 * An upload is an in-progress write to an object. An object may have multiple open uploads.
 * The upload is reflected on the object once it is committed. The upload is removed once it is
 * committed.
 */
public class CreateUploadRequest {

    private final String namespace;
    private final String bucketName;
    private final String objectName;

    /**
     * Data applied to the object to be created.
     */
    private final Map<String, String> metadata;

    /**
     * Checks to see if current eTag of the object matches this value.
     */
    private final String ifMatchEtag;

    /**
     * Checks to see if current eTag of the object does not match this value.
     */
    private final String ifNoneMatchEtag;

    public CreateUploadRequest(String namespace, String bucketName, String objectName,
                               Map<String, String> metadata, String ifMatchEtag,
                               String ifNoneMatchEtag) {
        this.namespace = namespace;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.metadata = metadata;
        this.ifMatchEtag = ifMatchEtag;
        this.ifNoneMatchEtag = ifNoneMatchEtag;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectName() {
        return objectName;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public String getIfMatchEtag() {
        return ifMatchEtag;
    }

    public String getIfNoneMatchEtag() {
        return ifNoneMatchEtag;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("namespace", namespace)
                .add("bucketName", bucketName)
                .add("objectName", objectName)
                .add("metadata", metadata)
                .add("ifMatchEtag", ifMatchEtag)
                .add("ifNoneMatchEtag", ifNoneMatchEtag)
                .toString();
    }
}
