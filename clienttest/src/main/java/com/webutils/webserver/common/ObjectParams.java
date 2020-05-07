package com.webutils.webserver.common;

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

    protected long objectSizeInBytes;

    public ObjectParams(final String namespace, final String bucket, final String object, final String objectFilePath) {
        this.namespaceName = namespace;
        this.bucketName = bucket;
        this.objectName = object;

        this.objectFilePath = objectFilePath;

        this.hostName = null;
        this.opcClientRequestId = null;

        this.objectSizeInBytes = 0;
    }

    public abstract String constructRequest();

    public void setHostName(final String host) { hostName = host; }

    public void setOpcClientRequestId(final String clientRequestId) { opcClientRequestId = clientRequestId; }

    public String getFilePathName() { return objectFilePath; }

}
