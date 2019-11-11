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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ClientTest implements Runnable {

    private int serverConnId;
    private int clientConnId;

    private boolean writeSignalSent;
    private boolean statusSignalSent;

    private AtomicInteger clientCount;

    private Thread clientThread;

    private volatile WaitSignal writeDone;

    private MemoryManager memoryAllocator;

    private HttpResponseListener responseListener;
    private HttpParser httpParser;

    private HttpResponseCompleted httpResponseCb;

    WriteConnection writeConn;
    ClientConnection client;

    ClientTest(final ClientConnection clientConnection, final int myServerId, final int myTargetId, AtomicInteger threadCount) {

        serverConnId = myServerId;
        clientConnId = myTargetId;

        clientCount = threadCount;
        clientCount.incrementAndGet();

        client = clientConnection;
    }

    void start() {

        clientThread = new Thread(this);
        clientThread.start();

        httpResponseCb = new HttpResponseCompleted(this);
        responseListener = new HttpResponseListener(httpResponseCb);
        httpParser = new HttpParser(responseListener);

        memoryAllocator = new MemoryManager();
    }

    void stop() {
        try {
            clientThread.join(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("clientThread.join() failed: " + int_ex.getMessage());
        }

        httpParser = null;
        responseListener = null;
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

        writeConn = client.addNewTarget(clientConnId);

        System.out.println("ClientTest[" + writeConn.getTransactionId() + "] serverConnId: " + serverConnId + " clientConnId: " + clientConnId);

        if (client.connectTarget(writeConn, 100)) {
            ByteBuffer msgHdr = memoryAllocator.poolMemAlloc(MemoryManager.MEDIUM_BUFFER_SIZE, null);

            String tmp = buildRequestString();

            str_to_bb(msgHdr, tmp);
            System.out.println("ClientTest[" + writeConn.getTransactionId() + "] msgHdr " + msgHdr.position() + " " + msgHdr.remaining());

            int bytesToWrite = msgHdr.position();
            msgHdr.flip();

            // Setup the read callback before sending any data
            TestClientReadCallback readDataCb = new TestClientReadCallback(this, httpParser);

            client.registerClientReadCallback(writeConn, readDataCb);

            // Send the message
            statusSignalSent = false;
            writeSignalSent = false;

            writeHeader(msgHdr, bytesToWrite);

            clientTestStep_1();

            memoryAllocator.poolMemFree(msgHdr);

            waitForStatus();

            System.out.println("ClientTest[" + writeConn.getTransactionId() + "] run(1): ");

            client.disconnectTarget(writeConn);
        }

        clientCount.decrementAndGet();
    }

    void userWriteCompleted(int result) {

        System.out.println("ClientTest[" + writeConn.getTransactionId() + "]userWriteComp(): " + result);

        synchronized (writeDone) {
            writeSignalSent = true;
            writeDone.notify();
        }
    }

    void resetWriteWaitFlag() {
        writeSignalSent = false;
    }

    boolean waitForWriteToComp() {
        boolean status = true;

        synchronized (writeDone) {

            writeSignalSent = false;
            while (!writeSignalSent) {
                try {
                    writeDone.wait(100);
                } catch (InterruptedException int_ex) {
                    int_ex.printStackTrace();
                    status = false;
                    break;
                }
            }
        }

        System.out.println("ClientTest[" + writeConn.getTransactionId() + "] waitForWrite() done: " + status);

        return status;
    }

    void statusReceived(int result) {
        System.out.println("ClientTest[" + writeConn.getTransactionId() + "]  statusReceived() : " + result);

        synchronized (writeDone) {
            statusSignalSent = true;
            writeDone.notify();
        }
    }


    boolean waitForStatus() {
        boolean status = true;

        synchronized (writeDone) {

            statusSignalSent = false;
            while (!statusSignalSent) {
                try {
                    writeDone.wait(100);
                } catch (InterruptedException int_ex) {
                    int_ex.printStackTrace();
                    status = false;
                    break;
                }
            }
        }

        System.out.println("ClientTest[" + writeConn.getTransactionId() + "] waitForStatus() done: " + status);

        return status;
    }

    static class WaitSignal {
        int count;
    }

    private void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(CharBuffer.wrap(in), out, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
     ** These are classes the various tests need to provide to change the test case behavior.
     */
    abstract String buildRequestString();

    abstract void clientTestStep_1();

    abstract void writeHeader(ByteBuffer msgHdr, int bytesToWrite);

    abstract void targetResponse(final int result, final ByteBuffer readBuffer);

    abstract void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted);
}
