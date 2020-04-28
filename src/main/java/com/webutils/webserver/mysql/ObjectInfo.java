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

    private int contentLength;
    private String contentMd5;

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
}
