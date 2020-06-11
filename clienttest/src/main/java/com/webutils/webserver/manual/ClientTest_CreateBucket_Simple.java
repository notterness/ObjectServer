package com.webutils.webserver.manual;

import com.webutils.webserver.common.Sha256Digest;
import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.CreateBucketPostContent;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.niosockets.NioTestClient;
import org.eclipse.jetty.http.HttpStatus;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_CreateBucket_Simple extends ClientTest {

    private String sha256Digest;

    private final String accessToken;

    ClientTest_CreateBucket_Simple(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
                          final int serverTcpPort, final String accessToken, final AtomicInteger testCount) {
        super(testName, client, serverIpAddr, serverTcpPort, testCount);

        this.accessToken = accessToken;
    }

    @Override
    public String buildRequestString(final String Md5Digest) {
        String contentStr = buildContent();

        return "POST /n/Tenancy-12345-abcde" + "" +
                "/b/ HTTP/1.1\n" +
                "Host: ClientTest-" + super.clientTestName + "\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "x-content-sha256: " + sha256Digest + "\n" +
                HttpInfo.ACCESS_TOKEN + ": " + accessToken + "\n" +
                HttpInfo.CONTENT_LENGTH + ": " + contentStr.length() + "\n\n" +
                contentStr;
    }

    @Override
    public String buildBufferAndComputeMd5() {
        return null;
    }

    /*
     ** In this test, the connection is closed after the HTTP Request is sent out
     */
    @Override
    public void targetResponse(final int result, final ByteBuffer readBuffer) {
        if ((result == 0) && (httpStatus == HttpStatus.OK_200)) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed httpStatus: " + httpStatus);
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }

    private String buildContent() {
        String contentString =
                "{\n" +
                "  \"" + ContentParser.COMPARTMENT_ID_ATTRIBUTE + "\": \"clienttest.compartment.12345.abcde\",\n" +
                "  \"" + CreateBucketPostContent.NAMESPACE_ATTRIBUTE + "\": \"Namespace-xyz-987\",\n" +
                "  \"" + CreateBucketPostContent.NAME_ATTRIBUTE + "\": \"CreateBucket_Simple\",\n" +
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
