package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.http.HttpResponseListener;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.server.ClientConnection;
import com.oracle.athena.webserver.server.WriteConnection;
import org.eclipse.jetty.http.HttpParser;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientTest implements Runnable {

    private int serverConnId;
    private int clientConnId;

    private boolean signalSent;

    private AtomicInteger clientCount;

    private Thread clientThread;

    private volatile WaitSignal writeDone;

    private MemoryManager memoryAllocator;

    private HttpResponseListener responseListener;
    private HttpParser httpParser;

    ClientTest(int myServerId, int myTargetId, AtomicInteger threadCount) {

        serverConnId = myServerId;
        clientConnId = myTargetId;

        clientCount = threadCount;
        clientCount.incrementAndGet();

        memoryAllocator = new MemoryManager();
    }

    void start() {

        clientThread = new Thread(this);
        clientThread.start();

        responseListener = new HttpResponseListener();
        httpParser = new HttpParser(responseListener);
    }

    /*
     ** This is the callback that is executed through the ClientWriteCompletion callback.
     **  ClientWriteCompletion extends the WriteCompletion callback to make it specific
     **  to the client who is performing the writes.
     */
    void writeCompleted(int result, ByteBuffer buffer) {

        userWriteCompleted(result);
    }

    /*
     ** TODO: Extend this to perform data writes after the header is sent.
     */
    public void run() {

        writeDone = new WaitSignal();

        System.out.println("ClientTest serverConnId: " + serverConnId + " clientConnId: " + clientConnId);

        // This sets up the server side of the connection
        ClientConnection client = new ClientConnection(memoryAllocator, serverConnId);
        client.start();

        WriteConnection writeConn = client.addNewTarget(clientConnId);

        if (client.connectTarget(writeConn, 100)) {
            ByteBuffer msgHdr = memoryAllocator.jniMemAlloc(MemoryManager.MEDIUM_BUFFER_SIZE);

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

            str_to_bb(msgHdr, tmp);
            System.out.println("msgHdr " + msgHdr.position() + " " + msgHdr.remaining());

            int bytesToWrite = msgHdr.position();
            msgHdr.flip();

            // Setup the read callback before sending any data
            TestClientReadCallback readDataCb = new TestClientReadCallback(httpParser);

            client.registerClientReadCallback(writeConn, readDataCb);

            // Send the message
            ClientWriteCompletion comp = new ClientWriteCompletion(this, writeConn, msgHdr, 1, bytesToWrite, 0);
            //client.writeData(writeConn, comp);

            waitForWriteToComp();

            memoryAllocator.jniMemFree(msgHdr);

            try {
                Thread.sleep(10000);
            } catch (InterruptedException int_ex) {

            }
            System.out.println("ClientTest run(1): ");

            client.disconnectTarget(writeConn);
        }

        client.stop();

        clientCount.decrementAndGet();
    }

    private void userWriteCompleted(int result) {

        System.out.println("ClientTest userWriteComp(): " + result);

        synchronized (writeDone) {
            signalSent = true;
            writeDone.notify();
        }
    }

    private boolean waitForWriteToComp() {
        boolean status = true;

        synchronized (writeDone) {

            signalSent = false;
            while (!signalSent) {
                try {
                    writeDone.wait(100);
                } catch (InterruptedException int_ex) {
                    int_ex.printStackTrace();
                    status = false;
                    break;
                }
            }
        }

        System.out.println("ClientTest waitForWriteDone(): " + status);

        return status;
    }

    static class WaitSignal {
        int count;
    }

    private void str_to_bb(ByteBuffer out, String in) {
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();

        try {
            boolean endOfInput = true;

            encoder.encode(CharBuffer.wrap(in), out, endOfInput);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
