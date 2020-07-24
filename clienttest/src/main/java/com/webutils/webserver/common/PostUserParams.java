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

public class PostUserParams extends ObjectParams {

    private String sha256Digest;

    private final String tenancyName;
    private final String customerName;
    private final String userName;
    private final String password;
    private final int permissions;

    public PostUserParams(final String tenancy, final String customer, final String userName, final String password,
                          final int permissions) {

        super(null, null, null, null);

        this.tenancyName = tenancy;
        this.customerName = customer;
        this.userName = userName;
        this.password = password;
        this.permissions = permissions;
        this.sha256Digest = null;
    }

    /*
     ** This builds the PostTenancy method headers. The following headers and if they are required:
     **
     **   namespaceName (required) "/c/" - This is set to indicate that the is to setup a customer tenancy.
     **
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   x-content-sha256 (required) - The computed Sha256 Digest for the content related to the bucket.
     **
     **   Content-Length (required) - The size in bytes of the bucket content information.
     **
     ** NOTE: The following are the parameters in the content that are required to create a Teanancy:
     **    Tenancy - This acts as the highest construct and provides an organization of all resources owned by a
     **      client (a client can also have multiple tenancies, but they are distinct and resources cannot be
     **      shared across tenancies).
     **    Customer Name - This is the owner of the Tenancy.
     **    Passphrase - This is a customer provided passphrase that is used to encrypt information related to this
     **      tenancy.
     */
    public String constructRequest() {
        String contentStr = buildContent();

        String request = "POST /u HTTP/1.1\n";

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

        request += HttpInfo.CONTENT_SHA256 + ": " + sha256Digest + "\n" +
                HttpInfo.CONTENT_LENGTH + ": " + contentStr.length() + "\n\n" +
                contentStr;

        return request;
    }

    /*
     ** This displays the results from the PostTenancy method.
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good or bad, then display the following:
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

        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            /*
             ** FIXME: At some point the response needs to be displayed for a successful CreateTenancy to echo
             **   back a bunch of the key value pairs.
             */
        } else {
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

    private String buildContent() {
        String contentString =
                "{\n" +
                        "  \"" + CreateTenancyPostContent.TENANCY_NAME_ATTRIBUTE + "\": \"" + tenancyName + "\",\n" +
                        "  \"" + CreateTenancyPostContent.CUSTOMER_ATTRIBUTE + "\": \"" + customerName + "\",\n" +
                        "  \"" + CreateUserPostContent.USER_NAME_ATTRIBUTE + "\": \"" +  userName + "\"\n" +
                        "  \"" + CreateUserPostContent.USER_PASSWORD_ATTRIBUTE + "\": \"" +  password + "\"\n" +
                        "  \"" + CreateUserPostContent.USER_PERMISSION_ATTRIBUTE + "\": \"" +  permissions + "\"\n" +
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
