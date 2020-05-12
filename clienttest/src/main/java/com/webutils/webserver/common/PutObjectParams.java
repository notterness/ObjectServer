package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpResponseInfo;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class PutObjectParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(PutObjectParams.class);

    /*
    ** computeMd5 digest is used to determine if the client wants an Md5 digest used for this object.
    **   If it is set to true, the Md5 digest will be computed for the file and passed to the Object Server.
    **   If it is set to false, the Md5 digest will not be used for this object.
     */
    private boolean computeMd5Digest;

    private AtomicBoolean md5DigestSet;
    private String md5Digest;

    public PutObjectParams(final String namespace, final String bucket, final String object, final String objectFilePath) {

        super(namespace, bucket, object, objectFilePath);

        this.computeMd5Digest = true;

        this.md5DigestSet = new AtomicBoolean(false);
        this.md5Digest = null;
    }

    /*
    ** This builds the PutObject request headers. The following headers and if they are required:
    **
    **   namespaceName (required) "/n/" - This is the namespace that holds the bucket where the object will be kept in.
    **   bucketName (required) "/b/" - This is the bucket that will hold the object.
    **   objectName (required) "/n/" - This is the name of the object where the file data will be retained within the
    **     Object Server.
    **   Host (required) - Who is sending the request.
    **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
    **     then to track their requests.
    **   if-none-match (not required) - This can only be set to "*". If it is set to "*" and there is already an object
    **     that matches the namespaceName/bucketName/objectName then an error will be returned. If this is not set and
    **     there is a matching object, a new record will be created with a higher version number. It is possible for
    **     the same object to have multiple versions.
    **   Content-MD5 (not required) - The computed Md5 Digest for the file being uploaded to the Object Server.
    **
    **   Content-Length (required) - The size in bytes of the file being uploaded
    **
    ** NOTE: This will be noted in multiple places. The hierarchy of how an object is saved is the following:
    **    Tenancy - This acts as the highest construct and provides an organization of all resources owned by a
    **      client (a client can also have multiple tenancies, but they are distinct and resources cannot be
    **      shared across tenancies).
    **    Namespace - Each region within a tenancy will have a unique namespace where all the buckets within a region
    **      are placed.
    **    Bucket - A client can create as many buckets as they desire within a namespace. The buckets provide a method
    **      to group objects.
     */
    public String constructRequest() {
        String initialContent = "PUT /n/" + namespaceName +
                "/b/" + bucketName +
                "/o/" + objectName + " HTTP/1.1\n";

        String finalContent = "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: ClientRequest/0.0.1\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n";

        String request;
        if (hostName != null) {
            request = initialContent + "Host: " + hostName + "\n";
        } else {
            request = initialContent + "Host: ClientTest\n";
        }

        if (opcClientRequestId != null) {
            request += "opc-client-request-id: " + opcClientRequestId + "\n";
        }

        if (ifNoneMatch != null) {
            request += "if-none-match: *\n";
        }

        if (computeMd5Digest && md5DigestSet.get()) {
            finalContent += "Content-MD5: " + md5Digest + "\n" +
                    "Content-Length: " + objectSizeInBytes + "\n\n";
        } else {
            finalContent += "Content-Length: " + objectSizeInBytes + "\n\n";
        }

        request += finalContent;

        return request;
    }

    /*
    ** This displays the results from the PutObject method.
    **
    ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
    **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
        ** If the status is good, then display the following:
        **   opc-client-request-id
        **   opc-request-id
        **   opc-content-md5
        **   ETag
        **   last-modified
        **   version-id
         */
        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            System.out.println("Status: 200");
            String opcClientRequestId = httpInfo.getOpcClientRequestId();
            if (opcClientRequestId != null) {
                System.out.println("opc-clent-request-id: " + opcClientRequestId);
            }

            String opcRequestId = httpInfo.getOpcRequestId();
            if (opcRequestId != null) {
                System.out.println("opc-request-id: " + opcRequestId);
            }

            String contentMd5 = httpInfo.getResponseContentMd5();
            if (contentMd5 != null) {
                System.out.println("opc-content-md5: " + contentMd5);
            }

            String etag = httpInfo.getResponseEtag();
            if (etag != null) {
                System.out.println("ETag: " + etag);
            }

            String lastModified = httpInfo.getResponseLastModified();
            if (lastModified != null) {
                System.out.println("last-modified: " + lastModified);
            }

            String versionId = httpInfo.getResponseVersionId();
            if (versionId != null) {
                System.out.println("version-id: " + versionId);
            }
        } else {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

    /*
    ** The following two routines are used when uploading a file to the Object Server. The md5DigestSet is an
    **   AtomicBoolean that is used to allow multiple threads to access the Md5 Digest information in a safe
    **   manner.
    ** The computeMd5Digest is used by the client to tell if they want the uploaded file to have its Md5 Digest
    **   computed and then validated by the Object Server. The default is to have the Md5 Digest computed and used as
    **   it provides a level a checking that the information transferred is valid.
    **
    ** NOTE: There is no accessor function for the md5Digest as it is only used when creating the request headers.
     */
    public boolean getComputeMd5Digest() { return computeMd5Digest; }
    public void setComputeMd5Digest() { computeMd5Digest = true; }

    public boolean getMd5DigestSet() { return md5DigestSet.get(); }

    public void setMd5Digest(final String objectMd5Digest) {
        md5Digest = objectMd5Digest;
        md5DigestSet.set(true);

        LOG.info("File: " + objectFilePath + " Md5 Digest: " + objectMd5Digest);
    }

    /*
    ** This validates that the file the client wants to upload is present and has a size greater than 0.
     */
    public long setObjectSizeInBytes() {
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

        return objectSizeInBytes;
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
