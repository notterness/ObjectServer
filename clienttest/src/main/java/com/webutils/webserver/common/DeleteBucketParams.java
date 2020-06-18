package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import org.eclipse.jetty.http.HttpStatus;

public class DeleteBucketParams extends ObjectParams {

    private final String accessToken;

    public DeleteBucketParams(final String namespace, final String bucket, final String accessToken) {

        super(namespace, bucket, null, null);

        this.accessToken = accessToken;
    }

    /*
     ** This builds the DeleteObject request headers. The following headers and if they are required:
     **
     **   namespaceName (required) "/n/" - This is the namespace that holds the bucket where the object will be kept in.
     **   bucketName (required) "/b/" - This is the bucket that will hold the object.
     **   objectName (required) "/n/" - This is the name of the object where the file data will be retained within the
     **     Object Server.
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   if-match (not required) - This is used to contain the specific ETag (entity tag - a unique ID associated with
     **     every object that is present in the Object Store) of the object to upload.
     **   versionId (not required) - This is set to the specific version of the object the client wants returned. If it
     **     is not set, then the most recent version of the object (i.e. object with the highest version number) will
     **     be returned.
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
        String initialContent = "DELETE /n/" + namespaceName +
                "/b/" + bucketName +
                " HTTP/1.1\n";

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

        if (accessToken != null) {
            request += HttpInfo.ACCESS_TOKEN + ": " + accessToken + "\n";
        }

        finalContent += HttpInfo.CONTENT_LENGTH + ": 0\n\n";

        request += finalContent;

        return request;
    }

    /*
     ** This displays the results from the DeleteObject method.
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good (DeleteBucket returns 204 if successful), then display the following:
         **   opc-client-request-id
         **   opc-request-id
         */
        int status = httpInfo.getResponseStatus();

        System.out.println("Status: " + status);
        String opcClientRequestId = httpInfo.getOpcClientRequestId();
        if (opcClientRequestId != null) {
            System.out.println(HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId);
        }

        String opcRequestId = httpInfo.getOpcRequestId();
        if (opcRequestId != null) {
            System.out.println(HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId);
        }

        if (httpInfo.getResponseStatus() == HttpStatus.METHOD_NOT_ALLOWED_405) {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String allowableMethods = httpInfo.getAllowableMethods();
            if (allowableMethods != null) {
                System.out.println("Allowed Methods: " + allowableMethods);
            }
        } else if (status != HttpStatus.NO_CONTENT_204){
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

}
