package com.webutils.webserver.manual;

import com.webutils.webserver.client.NioTestClient;
import com.webutils.webserver.common.Md5Digest;
import com.webutils.webserver.memory.MemoryManager;
import org.eclipse.jetty.http.HttpStatus;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_BadMd5 extends ClientTest {

    private final int BYTES_IN_CONTENT = MemoryManager.XFER_BUFFER_SIZE;

    private final Md5Digest digest;

    ClientTest_BadMd5(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
                      final int serverTcpPort, AtomicInteger testCount) {
        super(testName, client, serverIpAddr, serverTcpPort, testCount);

        digest = new Md5Digest();
    }

    @Override
    public String buildBufferAndComputeMd5() {
        /*
         ** Setup the 1kB data transfer here
         */
        String objectDigestString = null;

        objectBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, null);
        if (objectBuffer != null) {
            // Fill in a pattern
            long pattern = MemoryManager.XFER_BUFFER_SIZE;
            for (int i = 0; i < MemoryManager.XFER_BUFFER_SIZE; i = i + 8) {
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

        memoryManager.poolMemFree(objectBuffer, null);

        statusReceived(result);
    }

}
