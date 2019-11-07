package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.server.ClientConnection;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientTest_2 extends ClientTest {

    ClientTest_2(final ClientConnection client, final int myServerId, final int myTargetId, AtomicInteger threadCount) {
        super(client, myServerId, myTargetId, threadCount);
    }

    @Override
    String buildRequestString() {
        String tmp = new String("POST / HTTP/1.1\n" +
                "Host: iaas.us-phoenix-1.oraclecloud.com\n" +
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

        return tmp;
    }

    @Override
    void writeHeader(ByteBuffer msgHdr, int bytesToWrite) {
        // Send the message
        ClientWriteCompletion comp = new ClientWriteCompletion(this, writeConn, msgHdr, 1, bytesToWrite, 0);
        client.writeData(writeConn, comp);

        if (!waitForWriteToComp()) {
            System.out.println("Request timed out");
        }
    }


    @Override
    void clientTestStep_1() {
        //Do nothing in this test case
    }

    /*
     ** In this test, the full HTTP message is written and then a response is expected from the server.
     ** The response must have a result code of 200, indicating success.
     */
    @Override
    void targetResponse(final int result, final ByteBuffer readBuffer) {
        if (result == 0) {
            System.out.println("ClientTest_2 passed");
        } else {
            System.out.println("ClientTest_2 failed");
        }
    }

    @Override
    void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {

    }

}