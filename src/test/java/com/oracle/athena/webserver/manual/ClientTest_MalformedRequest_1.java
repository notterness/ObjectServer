package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.TestClient;
import org.eclipse.jetty.http.HttpStatus;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientTest_MalformedRequest_1 extends ClientTest {

    ClientTest_MalformedRequest_1(final String testName, final TestClient client, final int myServerId, final int myTargetId, AtomicInteger threadCount) {
        super(testName, client, myServerId, myTargetId, threadCount);
    }

    @Override
    String buildRequestString() {
        String tmp = new String("PUT / HTTP/9.9\n" +
                "Host: ClientTest-" + super.clientTestName + "\n" +
                "Content-Type: Something Random\n" +
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

        return tmp;
    }


    /*
     ** In this test, the full HTTP message is written and then a response is expected from the server.
     ** The response must have a result code of 200, indicating success.
     */
    @Override
    void targetResponse(final int result, final ByteBuffer readBuffer) {
        if ((result == 0) && (super.httpStatus == HttpStatus.BAD_REQUEST_400)) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed httpStatus: " + super.httpStatus);
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }
}
