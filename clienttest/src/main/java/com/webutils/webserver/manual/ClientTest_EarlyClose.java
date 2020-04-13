package com.webutils.webserver.manual;

import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.operations.OperationTypeEnum;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_EarlyClose extends ClientTest {

    private final OperationTypeEnum operationType = OperationTypeEnum.CLIENT_TEST_EARLY_CLOSE;


    ClientTest_EarlyClose(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
                          final int serverTcpPort, AtomicInteger testCount) {
        super(testName, client, serverIpAddr, serverTcpPort, testCount);
    }

    @Override
    public String buildRequestString(final String Md5Digest) {
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
                "Content-Length: 187\n\n" +
                "{\n" +
                "  \"cidrBlock\": \"172.16.0.0/16\",\n" +
                "  \"compartmentId\": \"ocid1.compartment.oc1..aaaaaaaauwjnv47knr7uuuvqar5bshnspi6xoxsfebh3vy72fi4swgrkvuvq\",\n" +
                "  \"displayName\": \"Apex Virtual Cloud Network\"\n" +
                "}\n\r\n");
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
}
