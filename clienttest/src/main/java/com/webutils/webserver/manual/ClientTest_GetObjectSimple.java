package com.webutils.webserver.manual;

import com.webutils.webserver.common.Md5Digest;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.operations.OperationTypeEnum;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientTest_GetObjectSimple extends ClientTest {

    private final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_TEST_GET_OBJECT_SIMPLE;

    private final int BYTES_IN_CONTENT = MemoryManager.XFER_BUFFER_SIZE;

    private final Md5Digest digest;

    ClientTest_GetObjectSimple(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
                        final int serverTcpPort, AtomicInteger testCount) {
        super(testName, client, serverIpAddr, serverTcpPort, testCount);

        digest = new Md5Digest();
        objectBuffer = null;
    }

    @Override
    public String buildBufferAndComputeMd5() {
        return null;
    }

    @Override
    public String buildRequestString(final String Md5_Digest) {
        return new String("GET /n/Namespace-xyz-987" + "" +
                "/b/CreateBucket_Simple" +
                "/o/5e223890-ea13-11e9-851d-234132e0fb02 HTTP/1.1\n" +
                "Host: ClientTest-" + super.clientTestName + "\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-Length: 0\n\n");
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
