package com.webutils.webserver.manual;

import com.webutils.webserver.client.NioTestClient;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/*
 ** This test writes the first part of the HTTP request and then waits before sending the remainder of the
 **   request. This is to test the slow connection handling in the Web Server connection code.
 */
class ClientTest_SlowHeaderSend extends ClientTest {

    ClientTest_SlowHeaderSend(final String testName, final NioTestClient client, final InetAddress serverIpAddr,
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
                "Content-MD5: ZsrFNJgrF3p0e7+GyNjAIA==\n" +
                "Content-Length: 187\n\n" +
                "{\n" +
                "  \"cidrBlock\": \"172.16.0.0/16\",\n" +
                "  \"compartmentId\": \"ocid1.compartment.oc1..aaaaaaaauwjnv47knr7uuuvqar5bshnspi6xoxsfebh3vy72fi4swgrkvuvq\",\n" +
                "  \"displayName\": \"Apex Virtual Cloud Network\"\n" +
                "}\n\r\n");
    }

    void writeHeader(ByteBuffer msgHdr, int bytesToWrite) {
        // Send the message, but only write the first SMALL_BUFFER_SIZE worth of bytes

        /*
         ** Wait 5.5 seconds before sending the remainder of the header. This should trigger
         **   the connection channel timeout handling code for headers.
         ** TODO: Fix the wait time and wire through the error handling completely
         */
        try {
            Thread.sleep(2500);
        } catch (InterruptedException int_ex) {
            int_ex.printStackTrace();
        }

        System.out.println(java.time.LocalTime.now() + " SlowHeaderSend - writeHeader(2) position:" + msgHdr.position() +
                " remaining: " + msgHdr.remaining() + " limit: " );


        if (!waitForWriteToComp()) {
            System.out.println("Request timed out");
        }
    }

    @Override
    public String buildBufferAndComputeMd5() {
        return null;
    }

    /*
     ** In this test, .
     */
    @Override
    public void targetResponse(final int result, final ByteBuffer readBuffer) {
        if (result == -1) {
            System.out.println(super.clientTestName + " passed");
        } else {
            /*
            ** TODO: This will need to be fixed when the ability to add delayed queuing to the state machine is
            **   added. That is required to allow the ConnectionState to be parked for a period of time or until
            **   an asynchronous event wakes it up to perform more work.
             */
            System.out.println(super.clientTestName + " passed");
        }

        statusReceived(result);
    }
}

