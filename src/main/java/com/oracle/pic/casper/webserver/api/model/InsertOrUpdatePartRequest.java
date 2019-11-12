package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;

/**
 * Request payload for part creation.
 * A part is a chunk of data that belongs to an upload of a multipart object.
 */
public class InsertOrUpdatePartRequest {
    private final String namespace;
    private final String bucketName;
    private final String objectName;

    /**
     * Unique identifier for an upload.
     */
    private final String uploadId;

    /**
     * Unique identifier for a part.
     */
    private final int uploadPartNum;

    /**
     * Checks to see if current eTag of the object matches this value.
     */
    private final String ifMatchEtag;

    /**
     * Checks to see if current eTag of the object does not match this value.
     */
    private final String ifNoneMatchEtag;

    public InsertOrUpdatePartRequest(
            String namespace, String bucketName, String objectName, String uploadId,
            int uploadPartNum, String ifMatchEtag, String ifNoneMatchEtag) {
        this.namespace = namespace;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.uploadId = uploadId;
        this.uploadPartNum = uploadPartNum;
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

    public String getUploadId() {
        return uploadId;
    }

    public int getUploadPartNum() {
        return uploadPartNum;
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
                .add("uploadId", uploadId)
                .add("uploadPartNum", uploadPartNum)
                .add("ifMatchEtag", ifMatchEtag)
                .add("ifNoneMatchEtag", ifNoneMatchEtag)
                .toString();
    }
}

