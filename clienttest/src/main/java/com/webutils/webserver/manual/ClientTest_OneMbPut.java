package com.webutils.webserver.manual;

import com.webutils.webserver.operations.NioTestClient;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;


class ClientTest_OneMbPut extends ClientTest {

    private final int BYTES_IN_CONTENT = 10240;

    ClientTest_OneMbPut(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
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
                "Content-Length: " + BYTES_IN_CONTENT + "\n\n");
    }

    void clientTestStep_1() {
        /*
         ** Wait a second before sending the content. This is to make debugging the state machine
         ** a bit easier.
         */
        try {
            Thread.sleep(1000);
        } catch (InterruptedException int_ex) {
            int_ex.printStackTrace();
        }

        /*
        ** Send out the 1MB data transfer here
         */
    }

    @Override
    public String buildBufferAndComputeMd5() {
        return null;
    }

    /*
    ** In this test,
     */
    @Override
    public void targetResponse(final int result, final ByteBuffer readBuffer) {
        if (result == -1) {
            System.out.println(super.clientTestName + " failed");
            super.client.setTestFailed(super.clientTestName);
        } else {
            System.out.println(super.clientTestName + " passed");
        }

        statusReceived(result);
    }

}
