package com.webutils.webserver.manual;

import com.webutils.webserver.client.NioTestClient;
import org.eclipse.jetty.http.HttpStatus;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientTest_2 extends ClientTest {

    ClientTest_2(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
                 final int serverTcpPort, AtomicInteger testCount) {
        super(testName, client, serverIpAddr, serverTcpPort, testCount);
    }

    @Override
    public String buildRequestString(final String Md5Digest) {
        return new String("PUT / HTTP/1.1\n" +
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

    /*
     ** In this test, the full HTTP Request is written and then a response is expected from the server.
     ** The response must have a result code of 400 since several required fields are missing in the
     **   request.
     */
    @Override
    void targetResponse(final int result, final ByteBuffer readBuffer) {
        if ((result == 0) && (super.httpStatus ==  HttpStatus.BAD_REQUEST_400)) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed");
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }

    @Override
    public String buildBufferAndComputeMd5() {
        return null;
    }
}