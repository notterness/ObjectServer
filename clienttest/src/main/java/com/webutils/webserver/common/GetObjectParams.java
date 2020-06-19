package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import org.eclipse.jetty.http.HttpStatus;

/*
** These are the parameters needed to send the GET object command to the Object Server
 */
public class GetObjectParams extends ObjectParams {

    private final String accessToken;

    public GetObjectParams(final String namespace, final String bucket, final String object, final String objectFilePath,
                           final String accessToken) {

        super(namespace, bucket, object, objectFilePath);

        this.accessToken = accessToken;
    }

    /*
     ** This builds the GetObject request headers. The following headers and if they are required:
     **
     **   namespaceName (required) "/n/" - This is the namespace that holds the bucket where the object will be kept in.
     **   bucketName (required) "/b/" - This is the bucket that will hold the object.
     **   objectName (required) "/n/" - This is the name of the object to be uploaded from the Object Server.
     **
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   versionId (not required) - This is set to the specific version of the object the client wants returned. If it
     **     is not set, then the most recent version of the object (i.e. object with the highest version number) will
     **     be returned.
     **   if-match (not required) - This is used to contain the specific ETag (entity tag - a unique ID associated with
     **     every object that is present in the Object Store) of the object to upload.
     **
     **   Content-Length (required) - Must be set to 0
     **
     ** NOTE: If both versionId and if-match are set, then the if-match will override the versionId matching.
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
        String initialContent = "GET /n/" + namespaceName +
                "/b/" + bucketName +
                "/o/" + objectName + " HTTP/1.1\n";

        String finalContent = "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: ClientRequest/0.0.1\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-Length: 0\n\n";

        String request;
        if (hostName != null) {
            request = initialContent + "Host: " + hostName + "\n";
        } else {
            request = initialContent + "Host: ClientTest\n";
        }

        if (opcClientRequestId != null) {
            request += HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId + "\n";
        }

        if (versionId != null) {
            request += HttpInfo.VERSION_ID + ": " + versionId + "\n";
        }

        if (ifMatch != null) {
            request += HttpInfo.IF_MATCH + ": " + ifMatch + "\n";
        }

        /*
        ** The accessToken is required to allow the ObjectGet method to be executed.
         */
        if (accessToken != null) {
            request += HttpInfo.ACCESS_TOKEN + ": " + accessToken + "\n";
        }

        request += finalContent;

        return request;
    }

    /*
    ** This outputs the response headers and response body for the GetObject command.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good, then display the following:
         **   opc-client-request-id
         **   opc-request-id
         **   ETag
         **   content-length
         **   content-md5
         **   last-modified
         **   version-id
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

        String etag = httpInfo.getResponseEtag();
        if (etag != null) {
            System.out.println(HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + etag);
        }

        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            int contentLength = httpInfo.getContentLength();
            System.out.println(HttpInfo.CONTENT_LENGTH + ": " + contentLength);

            String contentMd5 = httpInfo.getResponseContentMd5();
            if (contentMd5 != null) {
                System.out.println(HttpResponseInfo.OPC_CONTENT_MD5 + ": " + contentMd5);
            }

            String lastModified = httpInfo.getResponseLastModified();
            if (lastModified != null) {
                System.out.println(HttpResponseInfo.RESPONSE_LAST_MODIFIED + ": " + lastModified);
            }

            String versionId = httpInfo.getResponseVersionId();
            if (versionId != null) {
                System.out.println(HttpResponseInfo.RESPONSE_VERSION_ID + ": " + versionId);
            }
        } else {
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

}

