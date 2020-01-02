package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.client.NioTestClient;
import com.oracle.athena.webserver.client.SetupClientConnection;
import com.oracle.athena.webserver.http.HttpResponseListener;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.EventPollThread;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.eclipse.jetty.http.HttpParser;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ClientTest {

    private static final WebServerFlavor webServerFlavor = WebServerFlavor.INTEGRATION_TESTS;

    private int serverTcpPort;

    private boolean writeSignalSent;
    private boolean statusSignalSent;

    /*
    ** The runningTestCount is used by the caller to keep track of the number of tests that
    **   are currently running.
     */
    private final AtomicInteger runningTestCount;

    private Thread clientThread;

    private final Object writeDone;

    protected MemoryManager memoryManager;

    private HttpResponseListener responseListener;
    private HttpParser httpParser;

    protected final NioTestClient client;
    protected final EventPollThread eventThread;

    protected ByteBuffer objectBuffer;

    /*
    ** httpStatus is used in the sub-classes to validate that the correct response was
    **   returned from the Web Server code.
     */
    protected int httpStatus;
    protected final String clientTestName;

    ClientTest(final String testName, final NioTestClient testClient, final int serverTcpPort, AtomicInteger testCount) {

        this.serverTcpPort = serverTcpPort;

        this.runningTestCount = testCount;
        this.runningTestCount.incrementAndGet();

        /*
        ** The testClient is responsible for providing the threads the Operation(s) will run on and the
        **   NIO Socket handling.
         */
        this.client = testClient;
        this.eventThread = testClient.getEventThread();

        this.httpStatus = 0;

        this.clientTestName = testName;

        this.objectBuffer = null;

        this.writeDone = new Object();
    }

    /*
     **
     */
    public void execute() {

        /*
         ** Allocate a RequestContext
         */
        RequestContext clientContext = eventThread.allocateContext();

        /*
        ** Allocate an IoInterface to use
         */
        IoInterface connection = eventThread.allocateConnection(null);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException int_ex) {
            int_ex.printStackTrace();
        }

        /*
        ** Create the ClientHttpHeaderWrite operation and connect in this object to provide the HTTP header
        **   generator
         */
        memoryManager = new MemoryManager(webServerFlavor);
        SetupClientConnection setupClientConnection = new SetupClientConnection(webServerFlavor, clientContext, memoryManager,
                this, connection, serverTcpPort);
        setupClientConnection.initialize();

        /*
        ** Start the process of sending the HTTP Request and the request object
         */
        setupClientConnection.event();

        /*
        ** Now wait for the status to be received and then it can verified with the expected value
         */
        waitForStatus();

        /*
        ** Close out the SetupClientConnection Operation
         */
        setupClientConnection.complete();

        /*
        ** Release the resources back to the event thread (the owner of the RequestContext and IoInterface objects)
         */
        connection.closeConnection();
        eventThread.releaseConnection(connection);

        eventThread.releaseContext(clientContext);

        memoryManager.verifyMemoryPools(clientTestName);

        runningTestCount.decrementAndGet();
    }

    private void userWriteCompleted(int result) {

        System.out.println("ClientTest[" + clientTestName + "] userWriteComp(): " + result);

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

        System.out.println("ClientTest[" + clientTestName + "] waitForWrite() done: " + status);

        return status;
    }

    void statusReceived(int result) {
        System.out.println("ClientTest[" + clientTestName + "]  statusReceived() : " + result);

        synchronized (writeDone) {
            statusSignalSent = true;
            writeDone.notify();
        }
    }


    private boolean waitForStatus() {
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

        System.out.println("ClientTest[" + clientTestName + "] waitForStatus() done: " + status);

        return status;
    }

    public void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(CharBuffer.wrap(in), out, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
    **
     */
    public ByteBuffer getObjectBuffer() {
        return objectBuffer;
    }

    /*
     ** These are classes the various tests need to provide to change the test case behavior.
     */
    abstract public String buildBufferAndComputeMd5();

    abstract public String buildRequestString(final String Md5Digest);

    abstract void targetResponse(final int result, final ByteBuffer readBuffer);

    void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {
        if (headerCompleted) {
            httpStatus = status;

            targetResponse(0, null);
        }

        if (objectBuffer != null) {
            memoryManager.poolMemFree(objectBuffer);
        }
    }
}
