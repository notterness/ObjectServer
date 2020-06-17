package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.OperationTypeEnum;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


public class PutObjectMd5Params extends ObjectParamsWithData {

    private static final Logger LOG = LoggerFactory.getLogger(PutObjectMd5Params.class);

    /*
    ** The operationType is used to track the memory allocation in case there is a problem.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.PUT_OBJECT_MD5_PARAMS;

    public PutObjectMd5Params(final MemoryManager memoryManager, final String namespace, final String bucket,
                              final String object, final boolean validMd5, final String accessToken) {
        super(memoryManager, namespace, bucket, object, validMd5, accessToken);

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
     **     them to track their requests.
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
            request += HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId + "\n";
        }

        if (ifNoneMatch != null) {
            request += HttpInfo.IF_NONE_MATCH + ": *\n";
        }

        if (accessToken != null) {
            finalContent += HttpInfo.ACCESS_TOKEN + ": " + accessToken + "\n";
        }

        finalContent += HttpInfo.CONTENT_MD5 + ": " + computedMd5Digest + "\n" +
                    HttpInfo.CONTENT_LENGTH + ": " + objectSizeInBytes + "\n\n";

        request += finalContent;

        return request;
    }


    /*
    ** This builds up a simple pattern in a buffer that can be used to validate the Md5 checking.
     */
    public void constructRequestData() {
        ByteBuffer objectBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, null, operationType);
        if (objectBuffer != null) {
            // Fill in a pattern
            long pattern = MemoryManager.XFER_BUFFER_SIZE;
            for (int i = 0; i < MemoryManager.XFER_BUFFER_SIZE; i = i + 8) {
                objectBuffer.putLong(i, pattern);
                pattern++;
            }

            requestData.add(objectBuffer);
        } else {
            LOG.warn("Unable to allocate a buffer");
        }

        setObjectSizeInBytes(MemoryManager.XFER_BUFFER_SIZE);
        computeMd5Digest();
    }

    /*
     ** This displays the results from the PutObject method.
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good (AllocateChunks returns 200 if successful), then display the following:
         **   opc-client-request-id
         **   opc-request-id
         */
        System.out.println("Status: " + httpInfo.getResponseStatus());
        String opcClientRequestId = httpInfo.getOpcClientRequestId();
        if (opcClientRequestId != null) {
            System.out.println(HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId);
        }

        String opcRequestId = httpInfo.getOpcRequestId();
        if (opcRequestId != null) {
            System.out.println(HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId);
        }

        /*
        ** Different information is displayed depending upon the response status
         */
        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            System.out.println(HttpResponseInfo.OPC_CONTENT_MD5 + ": " + httpInfo.getResponseContentMd5());
            System.out.println(HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + httpInfo.getResponseEtag());
            System.out.println(HttpResponseInfo.RESPONSE_LAST_MODIFIED + ": " + httpInfo.getResponseLastModified());
        } else if (httpInfo.getResponseStatus() == HttpStatus.METHOD_NOT_ALLOWED_405) {
            String allowableMethods = httpInfo.getAllowableMethods();
            if (allowableMethods != null) {
                System.out.println("Allowed Methods: " + allowableMethods);
            }
        } else {
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

}
