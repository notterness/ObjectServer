package com.webutils.webserver.common;

import com.webutils.webserver.http.CreateTenancyPostContent;
import com.webutils.webserver.http.CreateUserPostContent;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class GetAccessTokenParams extends ObjectParams {

    private String sha256Digest;

    private final String tenancy;
    private final String customer;
    private final String user;
    private final String password;

    public GetAccessTokenParams(final String tenancy, final String customer, final String user, final String password) {

        super(null, null, null, null);

        this.tenancy = tenancy;
        this.customer = customer;
        this.user = user;
        this.password = password;

        this.sha256Digest = null;
    }

    public String constructRequest() {
        String contentStr = buildContent();

        String initialContent = "GET /t/" + tenancy + " HTTP/1.1\n";

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

        request += finalContent;

        request += HttpInfo.CONTENT_SHA256 + ": " + sha256Digest + "\n" +
                HttpInfo.CONTENT_LENGTH + ": " + contentStr.length() + "\n\n" +
                contentStr;


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
         **   accessToken
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
        ** This etag is the one associated with the user who the request is for.
         */
        String etag = httpInfo.getResponseEtag();
        if (etag != null) {
            System.out.println(HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + etag);
        }

        String accessToken = httpInfo.getAccessToken();
        if (accessToken != null) {
            System.out.println(HttpInfo.ACCESS_TOKEN + ":" + accessToken);
        }

        if (httpInfo.getResponseStatus() != HttpStatus.OK_200) {
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

    private String buildContent() {
        String contentString =
                "{\n" +
                        "  \"" + CreateTenancyPostContent.CUSTOMER_ATTRIBUTE + "\": \"" + customer + "\",\n" +
                        "  \"" + CreateUserPostContent.USER_NAME_ATTRIBUTE + "\": \"" +  user + "\"\n" +
                        "  \"" + CreateUserPostContent.USER_PASSWORD_ATTRIBUTE + "\": \"" +  password + "\"\n" +
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
