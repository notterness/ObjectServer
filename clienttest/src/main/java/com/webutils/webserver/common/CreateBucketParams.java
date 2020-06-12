package com.webutils.webserver.common;

import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.CreateBucketPostContent;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class CreateBucketParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(CreateBucketParams.class);

    private String sha256Digest;

    private final String tenancyName;
    private final String accessToken;

    public CreateBucketParams(final String tenancy, final String namespace, final String bucket, final String accessToken) {

        super(namespace, bucket, null, null);

        this.tenancyName = tenancy;
        this.accessToken = accessToken;
        this.sha256Digest = null;
    }

    /*
     ** This builds the PostBucket request headers. The following headers and if they are required:
     **
     **   namespaceName (required) "/n/" - This is the namespace that holds the bucket where the object will be kept in.
     **   bucketName (required) "/b/" - This is the bucket that will hold the object.
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   x-content-sha256 (required) - The computed Sha256 Digest for the content related to the bucket.
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
        String contentStr = buildContent();

        String request = "POST /n/" + tenancyName + "/b/ HTTP/1.1\n";

        if (hostName != null) {
            request += "Host: " + hostName + "\n";
        } else {
            request += "Host: ClientTest\n";
        }

        request += "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: ClientRequest/0.0.1\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n";

        if (opcClientRequestId != null) {
            request += HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId + "\n";
        }

        if (accessToken != null) {
            request += HttpInfo.ACCESS_TOKEN + ": " + accessToken + "\n";
        }

        request += HttpInfo.CONTENT_SHA256 + ": " + sha256Digest + "\n" +
                HttpInfo.CONTENT_LENGTH + ": " + contentStr.length() + "\n\n" +
                contentStr;

        return request;
    }

    /*
     ** This displays the results from the PostBucket method.
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good, then display the following:
         **   opc-client-request-id
         **   opc-request-id
         */
        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            System.out.println("Status: 200");
            String opcClientRequestId = httpInfo.getOpcClientRequestId();
            if (opcClientRequestId != null) {
                System.out.println(HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId);
            }

            String opcRequestId = httpInfo.getOpcRequestId();
            if (opcRequestId != null) {
                System.out.println(HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId);
            }
        } else {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

    private String buildContent() {
        String contentString =
                "{\n" +
                        "  \"" + ContentParser.COMPARTMENT_ID_ATTRIBUTE + "\": \"clienttest.compartment.12345.abcde\",\n" +
                        "  \"" + CreateBucketPostContent.NAMESPACE_ATTRIBUTE + "\": \"" + namespaceName + "\",\n" +
                        "  \"" + CreateBucketPostContent.NAME_ATTRIBUTE + "\": \"" + bucketName + "\",\n" +
                        "  \"" + CreateBucketPostContent.EVENTS_ENABLED_ATTRIBUTE + "\": false,\n" +
                        "  \"" + ContentParser.FREE_FORM_TAG + "\": {\"Test_1\": \"Test_2\"}, \n" +
                        "  \"" + ContentParser.DEFINED_TAGS + "\":\n" +
                        "  {\n" +
                        "    \"MyTags\":\n" +
                        "    {\n" +
                        "      \"TestTag_1\": \"ABC\", \n" +
                        "      \"TestTag_2\": \"123\", \n" +
                        "    }\n" +
                        "  }\n" +
                        "}\n";

        Sha256Digest digest = new Sha256Digest();

        /*
         ** The Sha-256 digest works on ByteBuffer, so the contentString needs to be pushed into a ByteBuffer and
         **   then the Sha-256 digest run on it.
         */
        ByteBuffer tmpBuffer = ByteBuffer.allocate(contentString.length());
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(CharBuffer.wrap(contentString), tmpBuffer, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        /*
         ** Now compute the Sha-256 digest
         */
        tmpBuffer.flip();
        digest.digestByteBuffer(tmpBuffer);
        sha256Digest = digest.getFinalDigest();

        return contentString;
    }

}