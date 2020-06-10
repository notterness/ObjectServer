package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpResponseInfo;

public abstract class ObjectParams {

    /*
     ** The namespaceName, bucketName and objectName are required parameters to construct the PUT Object request.
     **
     **   GET /n/{namespaceName}/b/{bucketName}/o/{objectName}
     */
    protected final String namespaceName;

    protected final String bucketName;

    protected final String objectName;

    /*
     ** The object is read from a file or written to a file. This is the full path and file name to access the file.
     */
    protected final String objectFilePath;

    protected String opcClientRequestId;
    protected String hostName;

    /*
    ** The versionId is used to version different instances of the same object
     */
    protected String versionId;

    /*
    **
     */
    protected String ifMatch;
    protected String ifNoneMatch;

    protected long objectSizeInBytes;

    public ObjectParams(final String namespace, final String bucket, final String object, final String objectFilePath) {
        this.namespaceName = namespace;
        this.bucketName = bucket;
        this.objectName = object;

        this.objectFilePath = objectFilePath;

        this.hostName = null;
        this.opcClientRequestId = null;

        this.objectSizeInBytes = 0;

        this.versionId = null;

        this.ifMatch = null;
        this.ifNoneMatch = null;
    }

    public abstract String constructRequest();
    public abstract void outputResults(final HttpResponseInfo httpInfo);

    public void setHostName(final String host) { hostName = host; }

    public void setOpcClientRequestId(final String clientRequestId) { opcClientRequestId = clientRequestId; }
    public String getOpcClientRequestId() { return opcClientRequestId; }

    public String getFilePathName() { return objectFilePath; }

    /*
    ** The user of the getObjectVersionId() needs to check for a null response and not do anything if it is set to null.
     */
    public void setObjectVersionId(final String version) { versionId = version; }
    public String getObjectVersionId() { return versionId; }

    /*
    **
     */
    public void setIfMatch(final String etag) { ifMatch = etag; }
    public void setIfNoneMatch() { ifNoneMatch = "*"; }
}
