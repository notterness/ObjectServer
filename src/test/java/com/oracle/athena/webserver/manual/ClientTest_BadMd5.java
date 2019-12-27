package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.NioTestClient;
import com.oracle.athena.webserver.common.Md5Digest;
import com.oracle.athena.webserver.memory.MemoryManager;
import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_BadMd5 extends ClientTest {

    private final int BYTES_IN_CONTENT = 1024;

    private final Md5Digest digest;

    private ByteBuffer dataBuffer;


    ClientTest_BadMd5(final String testName, final NioTestClient client, final int serverTcpPort, AtomicInteger testCount) {
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

        if (objectBuffer != null) {
            // Fill in a pattern
            long pattern = MemoryManager.MEDIUM_BUFFER_SIZE;
            for (int i = 0; i < MemoryManager.MEDIUM_BUFFER_SIZE; i = i + 8) {
                objectBuffer.putLong(i, pattern);
                pattern++;
            }

            digest.digestByteBuffer(objectBuffer);
            objectDigestString = digest.getFinalDigest();

            objectBuffer.rewind();

            System.out.println("MD5 Digest String: " + objectDigestString);
        }

        return objectDigestString;
    }

    /*
    ** The correct MD5 for the buffer is "Ye3L9i73DeNB8BhgjUXAhA==", note that the
    **   Content-MD5 value is different. First three characters are replaced by "abc".
     */
    @Override
    public String buildRequestString(final String Md5_Digest) {
        return new String("PUT /n/faketenantname" + "" +
                "/b/bucket-5e1910d0-ea13-11e9-851d-234132e0fb02" +
                "/o/5e223890-ea13-11e9-851d-234132e0fb02 HTTP/1.1\n" +
                "Host: ClientTest-" + super.clientTestName + "\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-MD5: abcL9i73DeNB8BhgjUXAhA==\n" +
                "Content-Length: " + BYTES_IN_CONTENT + "\n\n");
    }

    void clientTestStep_1() {
        /*
         ** Wait a 100mS before sending the content. This is to make debugging the state machine
         ** a bit easier.
         */
        try {
            Thread.sleep(100);
        } catch (InterruptedException int_ex) {
            int_ex.printStackTrace();
        }

        /*
         ** Send out the 1MB data transfer here
         */
        if (dataBuffer != null) {

            if (!waitForWriteToComp()) {
                System.out.println("Request timed out");
            }

            memoryAllocator.poolMemFree(dataBuffer);
        }
    }

    /*
     ** In this test, the full HTTP message is written and then a response is expected from the server.
     ** The response must have a result code of 422, indicating that the Md5 computation
     **   did not match the expected value passed in via the "Content-MD5" header.
     */
    @Override
    public void targetResponse(final int result, final ByteBuffer readBuffer) {
        if ((result == 0) && (super.httpStatus ==  HttpStatus.UNPROCESSABLE_ENTITY_422)) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed httpStatus: " + super.httpStatus);
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }

}
