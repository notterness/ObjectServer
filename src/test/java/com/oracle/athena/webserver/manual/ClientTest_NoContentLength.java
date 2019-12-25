package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.NioTestClient;
import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_NoContentLength extends ClientTest {

    ClientTest_NoContentLength(final String testName, final NioTestClient client, final int serverTcpPort, AtomicInteger testCount) {
        super(testName, client, serverTcpPort, testCount);
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
                "\n\r\n");
    }

    @Override
    public String buildBufferAndComputeMd5() {
        return null;
    }


    /*
     ** In this test, the full HTTP message is written and then a response is expected from the server.
     ** The response must have a result code of 200, indicating success.
     */
    @Override
    public void targetResponse(final int result, final ByteBuffer readBuffer) {
        if ((result == 0) && (super.httpStatus == HttpStatus.NO_CONTENT_204)) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed httpStatus: " + super.httpStatus);
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }

}
