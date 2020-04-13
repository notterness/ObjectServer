package com.webutils.webserver.manual;

import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.common.Md5Digest;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.OperationTypeEnum;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_CheckMd5 extends ClientTest {

    private final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_TEST_CHECK_MD5;

    private final int BYTES_IN_CONTENT = MemoryManager.XFER_BUFFER_SIZE;

    private final Md5Digest digest;

    ClientTest_CheckMd5(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
                        final int serverTcpPort, AtomicInteger testCount) {
        super(testName, client, serverIpAddr, serverTcpPort, testCount);

        digest = new Md5Digest();
        objectBuffer = null;
    }

    @Override
    public String buildBufferAndComputeMd5() {
        /*
         ** Setup the 1kB data transfer here
         */
        String objectDigestString = null;

        objectBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, null, operationType);
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
                "Content-MD5: " + Md5_Digest + "\n" +
                "Content-Length: " + BYTES_IN_CONTENT + "\n\n");
    }

    /*
     ** In this test, the full HTTP message is written and then a response is expected from the server.
     ** The response must have a result code of 200, indicating success.
     */
    @Override
    public void targetResponse(final int result, final ByteBuffer readBuffer) {
        if (result == 0) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed");
            super.client.setTestFailed(super.clientTestName);
        }

        /*
        ** Make sure to release the memory
         */
        memoryManager.poolMemFree(objectBuffer, null);
        objectBuffer = null;

        statusReceived(result);
    }

}
