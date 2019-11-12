package com.oracle.pic.casper.webserver.api.model;

import com.oracle.pic.casper.common.vertx.stream.BlobReadStream;
import java.util.Optional;

/**
 * POJO for creating a part against the backend.
 */
public class CreatePartRequest {

  private final String namespace;
  private final String bucketName;
  private final String objectName;
  private final String uploadId;
  private final int uploadPartNum;
  private final long sizeInBytes;
  private final BlobReadStream blobReadStream;

  private final Optional<String> ifMatchEtag;
  private final Optional<String> ifNoneMatchEtag;

  public CreatePartRequest(String namespace, String bucketName, String objectName,
                           String uploadId, int uploadPartNum, long sizeInBytes,
      BlobReadStream blobReadStream, Optional<String> ifMatchEtag, Optional<String> ifNoneMatchEtag) {
    this.namespace = namespace;
    this.bucketName = bucketName;
    this.objectName = objectName;
    this.uploadId = uploadId;
    this.uploadPartNum = uploadPartNum;
    this.sizeInBytes = sizeInBytes;
    this.blobReadStream = blobReadStream;
    this.ifMatchEtag = ifMatchEtag;
    this.ifNoneMatchEtag = ifNoneMatchEtag;
  }

  public long getSizeInBytes() {
    return sizeInBytes;
  }

  public int getUploadPartNum() {
    return uploadPartNum;
  }

  public String getUploadId() {
    return uploadId;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getObjectName() {
    return objectName;
  }

  public BlobReadStream getBlobReadStream() {
    return blobReadStream;
  }

  public Optional<String> getIfMatchEtag() {
    return ifMatchEtag;
  }

  public Optional<String> getIfNoneMatchEtag() {
    return ifNoneMatchEtag;
  }
}
