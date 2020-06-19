package com.webutils.webserver.common;

import com.webutils.webserver.http.*;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class CreateServerObjectParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(CreateServerObjectParams.class);

    private final String serverName;
    private final String ipAddr;
    private final int port;
    private final int numChunks;
    private final StorageTierEnum tier;

    private String sha256Digest;

    public CreateServerObjectParams(final String serverName, final String ipAddr, final int port, final int numChunks,
                                    final StorageTierEnum tier) {

        super(null, null, null, null);

        this.serverName = serverName;
        this.ipAddr = ipAddr;
        this.port = port;
        this.numChunks = numChunks;
        this.tier = tier;

        this.sha256Digest = null;
    }

    /*
     ** This builds the CreateServer POST method headers. The following headers and if they are required:
     **
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   x-content-sha256 - A sha256 digest of the content.
     **   Content-Length (required) - The size of the content that describes the server that is being created.
     **
     */
    public String constructRequest() {
        String initialContent = "POST / HTTP/1.1\n";

        String contentStr = buildContent();
        int contentSizeInBytes = contentStr.length();

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

        finalContent += HttpInfo.CONTENT_SHA256 + ": " + sha256Digest + "\n" +
                    HttpInfo.CONTENT_LENGTH + ": " + contentSizeInBytes + "\n\n" +
                    contentStr;

        request += finalContent;

        return request;
    }

    private String buildContent() {
        String contentString = new String(
                "{\n" +
                        "  \"" + ContentParser.SERVER_NAME + "\": \"" + serverName + "\",\n" +
                        "  \"\"" + ContentParser.SERVER_IP + "\": \"" + ipAddr + "\",\n" +
                        "  \"\"" + ContentParser.SERVER_PORT + "\": \"" + port + "\",\n" +
                        "  \"allocated-chunks\": \"" + numChunks + "\",\n" +
                        "  \"" + HttpRequestInfo.STORAGE_TIER_HEADER + "\": \"" + tier.toString() + "\",\n" +
                        "}\n");

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
         **   ETag
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

            String etag = httpInfo.getResponseEtag();
            if (etag != null) {
                System.out.println(HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + etag);
            }
        } else {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

}
