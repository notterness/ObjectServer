package com.webutils.webserver.common;

import com.webutils.storageserver.operations.ReadFromFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class PutObjectParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(PutObjectParams.class);

    /*
     ** The following are optional headers
     */
    private String versionId;


    private boolean computeMd5Digest;
    private String md5Digest;

    public PutObjectParams(final String namespace, final String bucket, final String object, final String objectFilePath) {

        super(namespace, bucket, object, objectFilePath);

        this.computeMd5Digest = true;
        this.md5Digest = null;
    }

    public String constructRequest() {
        String initialContent = "PUT /n/" + namespaceName +
                "/b/" + bucketName +
                "/o/" + objectName + " HTTP/1.1\n";

        String finalContent = "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: ClientRequest/0.0.1\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-MD5: " + md5Digest + "\n" +
                "Content-Length: " + objectSizeInBytes + "\n\n";

        String request;
        if (hostName != null) {
            request = initialContent + "Host: " + hostName + "\n";
        } else {
            request = initialContent + "Host: ClientTest\n";
        }

        if (opcClientRequestId != null) {
            request += "opc-client-request-id: " + opcClientRequestId + "\n";
        }

        request += finalContent;

        return request;
    }

    public boolean getComputeMd5Digest() { return computeMd5Digest; }

    public void setMd5Digest(final String objectMd5Digest) { md5Digest = objectMd5Digest; }

    /*
    ** This validates that the file is present and has a size greater than 0.
     */
    public boolean setObjectSizeInBytes() {
        objectSizeInBytes = 0;
        File inFile = new File(objectFilePath);
        try {
            FileChannel readFileChannel = new FileInputStream(inFile).getChannel();

            try {
                objectSizeInBytes = readFileChannel.size();
            } catch (IOException io_ex) {
                LOG.error("Unable to obtain file length - " + objectFilePath + " ex:" + io_ex.getMessage());
            }
        } catch (FileNotFoundException ex) {
            LOG.info("setObjectSizeInBytes()) file not found: " + objectFilePath + " ex:" + ex.getMessage());
        }

        LOG.info("setObjectSizeInBytes() bytesToReadFromFile: " + objectSizeInBytes);

        if (objectSizeInBytes == 0) {
            return false;
        }

        return true;
    }

    public boolean setObjectSizeInBytes(final long sizeInBytes) {
        /*
         ** Verify the sizes match
         */
        if ((objectSizeInBytes != 0) && (sizeInBytes != objectSizeInBytes)){
            LOG.warn("PutObjectParams object size mismatch objectSizeInBytes: " + objectSizeInBytes + " sizeInBytes: " +
                    sizeInBytes);
            objectSizeInBytes = 0;
            return false;
        }

        objectSizeInBytes = sizeInBytes;
        return true;
    }

}
