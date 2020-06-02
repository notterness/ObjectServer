package com.webutils.webserver.mysql;

import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;

import java.util.LinkedList;
import java.util.List;

/*
** This is a representation of the data stored in the ObjectStorageDb object table
 */
public class ObjectInfo {

    private final String namespace;
    private final String bucketName;
    private final String objectName;

    /*
    ** This is the list of all the chunks that have been saved off for this Object
     */
    private final List<ServerIdentifier> chunkList;

    private int objectId;

    private int contentLength;
    private String contentMd5;

    private String lastModified;
    private String versionId;

    private String etag;


    public ObjectInfo(final HttpRequestInfo objectHttpInfo) {

        namespace = objectHttpInfo.getNamespace();
        bucketName = objectHttpInfo.getBucket();
        objectName = objectHttpInfo.getObject();

        chunkList = new LinkedList<>();
    }

    public String getNamespace() { return namespace; }
    public String getBucketName() { return bucketName; }
    public String getObjectName() {
        return objectName;
    }

    public void setObjectId(final int uniqueId) { objectId = uniqueId; }
    public int getObjectId() { return objectId; }

    public void setContentLength(final int length) {
        contentLength = length;
    }
    public int getContentLength() {
        return contentLength;
    }

    public void setContentMd5(final String digest) {
        contentMd5 = digest;
    }
    public String getContentMd5() {
        return contentMd5;
    }

    public List<ServerIdentifier> getChunkList() {
        return chunkList;
    }

    public void setLastModified( final String date ) { lastModified = date; }
    public String getLastModified() { return lastModified; }

    public void setVersionId(final String id) { versionId = id; }
    public String getVersionId() { return versionId; }

    public void setEtag( final String objectUUID ) { etag = objectUUID; }
    public String getEtag() {return etag; }
}
