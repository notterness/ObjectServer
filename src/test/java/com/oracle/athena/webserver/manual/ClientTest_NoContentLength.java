package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.TestClient;
import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientTest_NoContentLength extends ClientTest {

    ClientTest_NoContentLength(final String testName, final TestClient client, final int myServerId, final int myTargetId, AtomicInteger threadCount) {
        super(testName, client, myServerId, myTargetId, threadCount);
    }

    @Override
    String buildRequestString() {
         String tmp = new String("PUT / HTTP/1.1\n" +
                "Host: iaas.us-phoenix-1.oraclecloud.com\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "\n\r\n");

        return tmp;
    }


    /*
     ** In this test, the full HTTP message is written and then a response is expected from the server.
     ** The response must have a result code of 200, indicating success.
     */
    @Override
    void targetResponse(final int result, final ByteBuffer readBuffer) {
        if ((result == 0) && (super.httpStatus == HttpStatus.NO_CONTENT_204)) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed httpStatus: " + super.httpStatus);
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }

}
