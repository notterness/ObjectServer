package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.NioTestClient;
import com.oracle.athena.webserver.common.Md5Digest;
import com.oracle.athena.webserver.memory.MemoryManager;
import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_MissingObjectName extends ClientTest {

    private final int BYTES_IN_CONTENT = MemoryManager.MEDIUM_BUFFER_SIZE;

    private final Md5Digest digest;

    private ByteBuffer dataBuffer;


    ClientTest_MissingObjectName(final String testName, final NioTestClient client, final int serverTcpPort, AtomicInteger testCount) {
        super(testName, client, serverTcpPort, testCount);

        digest = new Md5Digest();
        dataBuffer = null;
    }

    @Override
    public String buildBufferAndComputeMd5() {
        /*
         ** Setup the 1kB data transfer here
         */
        String objectDigestString = null;

        dataBuffer = memoryAllocator.poolMemAlloc(MemoryManager.MEDIUM_BUFFER_SIZE, null);
        if (dataBuffer != null) {
            // Fill in a pattern
            long pattern = MemoryManager.MEDIUM_BUFFER_SIZE;
            for (int i = 0; i < MemoryManager.MEDIUM_BUFFER_SIZE; i = i + 8) {
                dataBuffer.putLong(i, pattern);
                pattern++;
            }

            digest.digestByteBuffer(dataBuffer);
            objectDigestString = digest.getFinalDigest();

            dataBuffer.rewind();

            System.out.println("MD5 Digest String: " + objectDigestString);
        }

        return objectDigestString;
    }

    @Override
    public String buildRequestString(final String Md5_Digest) {
        return new String("PUT /n/faketenantname" + "" +
                "/b/bucket-5e1910d0-ea13-11e9-851d-234132e0fb02" +
                " HTTP/1.1\n" +
                "Host: ClientTest-" + super.clientTestName + "\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-MD5: " + Md5_Digest + "\n" +
                "Content-Length: " + BYTES_IN_CONTENT + "\n\n");
    }

    void clientTestStep_1() {
        /*
         ** Send out the 1kB data transfer here
         */
     }

    /*
     ** In this test, the full HTTP message is written and then a response is expected from the server.
     ** The response must have a result code of 200, indicating success.
     */
    @Override
    public void targetResponse(final int result, final ByteBuffer readBuffer) {
        if ((result == 0) && (super.httpStatus ==  HttpStatus.BAD_REQUEST_400)) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed");
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }

}
