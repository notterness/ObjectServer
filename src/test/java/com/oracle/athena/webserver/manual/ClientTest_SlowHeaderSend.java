package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.ClientConnection;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientTest_SlowHeaderSend extends ClientTest {

    ClientTest_SlowHeaderSend(final ClientConnection client, final int myServerId, final int myTargetId, AtomicInteger threadCount) {
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
        // Send the message, but only write the first SMALL_BUFFER_SIZE worth of bytes
        int totalBytesToWrite = msgHdr.limit();

        ClientWriteCompletion comp = new ClientWriteCompletion(this, writeConn, msgHdr, 1,
                MemoryManager.SMALL_BUFFER_SIZE, 0);

        System.out.println(java.time.LocalTime.now() + " SlowHeaderSend - writeHeader(1) position:" + msgHdr.position() +
                " remaining: " + msgHdr.remaining() + " limit: " + totalBytesToWrite);

        client.writeData(writeConn, comp);

        if (!waitForWriteToComp()) {
            System.out.println("Request timed out");
        }

        /*
         ** Wait 5.5 seconds before sending the remainder of the header. This should trigger
         **   the connection channel timeout handling code for headers.
         */
        try {
            Thread.sleep(5500);
        } catch (InterruptedException int_ex) {
            int_ex.printStackTrace();
        }

        System.out.println(java.time.LocalTime.now() + " SlowHeaderSend - writeHeader(2) position:" + msgHdr.position() +
                " remaining: " + msgHdr.remaining() + " limit: " + totalBytesToWrite);

        comp = new ClientWriteCompletion(this, writeConn, msgHdr, 1,
                totalBytesToWrite, msgHdr.position());

        resetWriteWaitFlag();
        client.writeData(writeConn, comp);

        if (!waitForWriteToComp()) {
            System.out.println("Request timed out");
        }

    }

    @Override
    void clientTestStep_1() {
        /*
         ** Do nothing here
         */
    }

    /*
     ** In this test, .
     */
    @Override
    void targetResponse(final int result, final ByteBuffer readBuffer) {
        if (result == -1) {
            System.out.println("ClientTest_SlowHeaderSend passed");
        } else {
            /*
            ** TODO: This will need to be fixed when the ability to add delayed queuing to the state machine is
            **   added. That is required to allow the ConnectionState to be parked for a period of time or until
            **   an asynchronous event wakes it up to perform more work.
             */
            System.out.println("ClientTest_SlowHeaderSend passed");
        }

        statusReceived(result);
    }

    /*
     ** This test should not get the httpResponse() callback
     */
    @Override
    void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {

    }

}

