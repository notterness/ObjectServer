package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.TestClient;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest_EarlyClose extends ClientTest {

    ClientTest_EarlyClose(final String testName, final TestClient client, final int myServerId, final int myTargetId, AtomicInteger threadCount) {
        super(testName, client, myServerId, myTargetId, threadCount);
    }

    @Override
    String buildRequestString() {
        return new String("POST / HTTP/1.1\n" +
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
    ** This write sends the first part of the message and then closes the channel
     */
    @Override
    void writeHeader(ByteBuffer msgHdr, int bytesToWrite) {
        // Send the message, but only write the first SMALL_BUFFER_SIZE worth of bytes

        ClientWriteCompletion comp = new ClientWriteCompletion(this, writeConn, msgHdr, 1,
                MemoryManager.SMALL_BUFFER_SIZE, 0);

        client.writeData(writeConn, comp);

        if (!waitForWriteToComp()) {
            System.out.println("Request timed out");
        }
    }

    @Override
    void clientTestStep_1() {
        writeConn.closeChannel();
    }

    /*
     ** In this test, .
     */
    @Override
    void targetResponse(final int result, final ByteBuffer readBuffer) {
        if (result == -1) {
            System.out.println(super.clientTestName + " passed");
        } else {
            System.out.println(super.clientTestName + " failed");
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }
}
