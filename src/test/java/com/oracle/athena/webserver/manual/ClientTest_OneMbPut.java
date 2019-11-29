package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.TestClient;
import com.oracle.athena.webserver.memory.MemoryManager;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;


public class ClientTest_OneMbPut extends ClientTest {

    private final int BYTES_IN_CONTENT = 10240;

    ClientTest_OneMbPut(final String testName, final TestClient client, final int myServerId, final int myTargetId, AtomicInteger threadCount) {
        super(testName, client, myServerId, myTargetId, threadCount);
    }

    @Override
    String buildRequestString() {
        return new String("PUT / HTTP/1.1\n" +
                "Host: ClientTest-" + super.clientTestName + "\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-Length: " + BYTES_IN_CONTENT + "\n\n");
    }

    @Override
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
        ByteBuffer dataBuffer = memoryAllocator.poolMemAlloc(MemoryManager.LARGE_BUFFER_SIZE, null);
        if (dataBuffer != null) {
            // Fill in a pattern
            long pattern = MemoryManager.LARGE_BUFFER_SIZE;
            for (int i = 0; i < MemoryManager.LARGE_BUFFER_SIZE; i = i + 8) {
                dataBuffer.putLong(i, pattern);
                pattern++;
            }

            dataBuffer.flip();

            ClientWriteCompletion comp = new ClientWriteCompletion(this, writeConn, dataBuffer, 1,
                    BYTES_IN_CONTENT, 0);

            System.out.println(java.time.LocalTime.now() + " OneMbPut - writeHeader(1) position:" + dataBuffer.position() +
                    " remaining: " + dataBuffer.remaining() + " limit: " + BYTES_IN_CONTENT);

            resetWriteWaitFlag();
            client.writeData(writeConn, comp);

            if (!waitForWriteToComp()) {
                System.out.println("Request timed out");
            }

            memoryAllocator.poolMemFree(dataBuffer);
        }
    }

    /*
    ** In this test, .
     */
    @Override
    void targetResponse(final int result, final ByteBuffer readBuffer) {
        if (result == -1) {
            System.out.println(super.clientTestName + " failed");
        } else {
            System.out.println(super.clientTestName + " passed");
            super.client.setTestFailed(super.clientTestName);
        }

        statusReceived(result);
    }

}
