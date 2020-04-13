package com.webutils.webserver.manual;

import com.webutils.webserver.common.Sha256Digest;
import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.operations.OperationTypeEnum;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_CreateBucket_Simple extends ClientTest {

    private final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_TEST_CREATE_BUCKET_SIMPLE;

    private String sha256Digest;

    ClientTest_CreateBucket_Simple(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
                          final int serverTcpPort, AtomicInteger testCount) {
        super(testName, client, serverIpAddr, serverTcpPort, testCount);
    }

    @Override
    public String buildRequestString(final String Md5Digest) {
        String contentStr = buildContent();

        return new String("POST /n/faketenantname" + "" +
                "/b/ HTTP/1.1\n" +
                "Host: ClientTest-" + super.clientTestName + "\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "x-content-sha256: " + sha256Digest + "\n" +
                "Content-Length: " + contentStr.length() + "\n\n" +
                contentStr);
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
        if (result == -1) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed");
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }

    private String buildContent() {
        String contentString = new String(
                "{\n" +
                        "  \"compartmentId\": \"clienttest.compartment.12345.abcde\",\n" +
                        "  \"namespace\": \"testnamespace\",\n" +
                        "  \"objectEventsEnabled\": false\n" +
                        "  \"freeformTags\": {\"Test_1\": \"Test_2\"}\", \n" +
                        "  \"definedTags\":\n" +
                        "  {\n" +
                        "    {\n" +
                        "      \"TestTag_1\": \"ABC\", \n" +
                        "      \"TestTag_2\": \"123\", \n" +
                        "    }\n" +
                        "  }\n" +
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
        System.out.println("start: " + tmpBuffer.position() + " limit: " + tmpBuffer.limit());
        digest.digestByteBuffer(tmpBuffer);
        sha256Digest = digest.getFinalDigest();

        System.out.println("sha256 digest: " + sha256Digest);

        return contentString;
    }
}
