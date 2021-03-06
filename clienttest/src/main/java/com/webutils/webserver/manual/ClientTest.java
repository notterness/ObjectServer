package com.webutils.webserver.manual;

import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.operations.SetupClientConnection;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ClientTest {

    private static final WebServerFlavor webServerFlavor = WebServerFlavor.CLI_CLIENT;

    private InetAddress serverIpAddr;
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

    protected final NioTestClient client;
    protected final EventPollThread eventThread;
    protected final int eventThreadId;

    protected ByteBuffer objectBuffer;

    /*
    ** httpStatus is used in the sub-classes to validate that the correct response was
    **   returned from the Web Server code.
     */
    protected int httpStatus;
    protected final String clientTestName;

    ClientTest(final String testName, final NioTestClient testClient, final InetAddress serverIpAddr,
               final int serverTcpPort, AtomicInteger testCount) {

        this.serverIpAddr = serverIpAddr;
        this.serverTcpPort = serverTcpPort;

        this.runningTestCount = testCount;
        this.runningTestCount.incrementAndGet();

        /*
        ** The testClient is responsible for providing the threads the Operation(s) will run on and the
        **   NIO Socket handling.
         */
        this.client = testClient;
        this.eventThread = testClient.getEventThread();
        this.eventThreadId = this.eventThread.getEventPollThreadBaseId();

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
        RequestContext clientContext = client.allocateContext(eventThreadId);

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

        ServerIdentifier serverId = new ServerIdentifier("ClientTest", serverIpAddr, serverTcpPort, 0);

        HttpResponseInfo httpResponseInfo = new HttpResponseInfo(clientContext.getRequestId());
        serverId.setHttpInfo(httpResponseInfo);
        SetupClientConnection setupClientConnection = new SetupClientConnection(clientContext, memoryManager,
                this, connection, serverId);
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

        client.releaseContext(clientContext);

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

    /*
    ** This is used to wake up the thread waiting for the test to complete.
     */
    void statusReceived(int result) {
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

        if (!status) {
            System.out.println("ClientTest[" + clientTestName + "] waitForStatus() timed out");
        }

        return status;
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
            memoryManager.poolMemFree(objectBuffer, null);
        }
    }
}
