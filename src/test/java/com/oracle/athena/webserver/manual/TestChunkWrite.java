package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.client.ChunkWriteTestComplete;
import com.oracle.athena.webserver.client.NioTestClient;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.EventPollThread;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.operations.SetupChunkWrite;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.athena.webserver.requestcontext.ServerIdentifier;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class TestChunkWrite {

    private static final Logger LOG = LoggerFactory.getLogger(TestChunkWrite.class);

    private static final WebServerFlavor webServerFlavor = WebServerFlavor.INTEGRATION_TESTS;

    private final int NUM_STORAGE_SERVER_WRITE_BUFFERS = 10;
    private final int NUMBER_OF_BYTES_TO_WRITE = MemoryManager.XFER_BUFFER_SIZE * 10;

    private final int storageServerTcpPort;

    private final AtomicInteger runningTestCount;

    protected final NioTestClient client;
    protected final EventPollThread eventThread;

    private boolean statusSignalSent;
    private final Object writeDone;


    TestChunkWrite(final NioTestClient testClient, final int storageServerTcpPort, AtomicInteger testCount) {

        this.storageServerTcpPort = storageServerTcpPort;
        this.runningTestCount = testCount;

        /*
         ** The testClient is responsible for providing the threads the Operation(s) will run on and the
         **   NIO Socket handling.
         */
        this.client = testClient;
        this.eventThread = testClient.getEventThread();

        /*
        ** This is used to manage the callback when the test is complete
         */
        this.statusSignalSent = false;
        this.writeDone = new Object();

        /*
        ** Increment the number of running tests
         */
        runningTestCount.incrementAndGet();
    }

    void execute() {
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
         *     public SetupChunkWrite(final RequestContext requestContext, final MemoryManager memoryManager,
                           final BufferManagerPointer encryptedBufferPtr,
                           final int chunkBytesToEncrypt) {

         */
        MemoryManager memoryManager = new MemoryManager(webServerFlavor);

        /*
        ** The unqiue identifier for this Chunk write test
         */
        ServerIdentifier chunkId = new ServerIdentifier(InetAddress.getLoopbackAddress(),
                RequestContext.STORAGE_SERVER_PORT_BASE, 0);


        /*
         ** The ChunkWriteTest operation is what is called back when the Chunk Write completes
         */
        ChunkWriteTestComplete testChunkWrite = new ChunkWriteTestComplete(clientContext, chunkId,  this);

        /*
        ** Fill in some buffers to pass into the chunk write
         */
        BufferManager storageServerWriteBufferMgr = clientContext.getStorageServerWriteBufferManager();
        BufferManagerPointer storageServerAddPointer = storageServerWriteBufferMgr.register(testChunkWrite);
        storageServerWriteBufferMgr.bookmark(storageServerAddPointer);

        int fillPattern = 1;
        for (int i = 0; i < NUM_STORAGE_SERVER_WRITE_BUFFERS; i++) {
            ByteBuffer writeBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, null);

            /*
            ** Write a known pattern into the buffers
             */
            for (int j = 0; j < MemoryManager.XFER_BUFFER_SIZE; j += 4) {
                writeBuffer.putInt(j, fillPattern);
                fillPattern++;
            }
            storageServerWriteBufferMgr.offer(storageServerAddPointer, writeBuffer);
        }

        BufferManagerPointer storageServerWritePtr = storageServerWriteBufferMgr.register(testChunkWrite, storageServerAddPointer);

        SetupChunkWrite setupChunkWrite = new SetupChunkWrite(clientContext, chunkId, memoryManager,
                storageServerWritePtr, NUMBER_OF_BYTES_TO_WRITE, testChunkWrite);
        setupChunkWrite.initialize();

        /*
         ** Start the process of setting up and writing a chunk to the Storage Server
         */
        setupChunkWrite.event();

        /*
         ** Now wait for the status to be received and then it can verified with the expected value
         */
        waitForStatus();

        /*
        ** Remove the write pointer dependency from the add pointer so all the buffers can
        **   be released.
         */
        storageServerWriteBufferMgr.unregister(storageServerWritePtr);

        /*
        ** Now free up all the memory allocated for the test
         */
        storageServerWriteBufferMgr.reset(storageServerAddPointer);
        for (int i = 0; i < NUM_STORAGE_SERVER_WRITE_BUFFERS; i++) {
            ByteBuffer buffer = storageServerWriteBufferMgr.poll(storageServerAddPointer);

            if (buffer != null) {
                memoryManager.poolMemFree(buffer);
            } else {
                LOG.info("TestChunkWrite storageServerAddPointer index: " + storageServerAddPointer.getCurrIndex());
            }
        }
        storageServerWriteBufferMgr.unregister(storageServerAddPointer);
        storageServerWriteBufferMgr.reset();

        eventThread.releaseConnection(connection);

        clientContext.reset();
        eventThread.releaseContext(clientContext);

        /*
        ** Verify all the ByteBuffer(s) were returned to the MemoryManager
         */
        memoryManager.verifyMemoryPools("TestChunkWrite");

        /*
        ** Let the higher level know this test is complete
         */
        runningTestCount.decrementAndGet();
    }

    public void statusReceived(int result) {
        System.out.println("TestChunkWrite  statusReceived() : " + result);

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

        System.out.println("TestChunkWrite waitForStatus() done: " + status);

        return status;
    }

}
