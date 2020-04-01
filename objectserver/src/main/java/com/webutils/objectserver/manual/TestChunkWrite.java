package com.webutils.objectserver.manual;

import com.webutils.objectserver.operations.SetupChunkWrite;
import com.webutils.objectserver.requestcontext.ObjectServerRequestContext;
import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
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

    private final InetAddress storageServerAddr;
    private final int storageServerTcpPort;

    private final AtomicInteger runningTestCount;

    protected final NioTestClient client;
    protected final EventPollThread eventThread;

    private boolean statusSignalSent;
    private final Object writeDone;


    TestChunkWrite(final NioTestClient testClient, final InetAddress addr, final int storageServerTcpPort,
                   AtomicInteger testCount) {

        this.storageServerAddr = addr;
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
        ObjectServerRequestContext clientContext = eventThread.allocateContext();

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
        ** The unique identifier for this Chunk write test
         */
        ServerIdentifier chunkId = new ServerIdentifier("TestChunkWrite", storageServerAddr, storageServerTcpPort, 0);

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
            ByteBuffer writeBuffer = memoryManager.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, storageServerWriteBufferMgr);

            /*
            ** Write a known pattern into the buffers
             */
            for (int j = 0; j < MemoryManager.XFER_BUFFER_SIZE; j += 4) {
                writeBuffer.putInt(j, fillPattern);
                fillPattern++;
            }
            storageServerWriteBufferMgr.offer(storageServerAddPointer, writeBuffer);
        }

        SetupChunkWrite setupChunkWrite = new SetupChunkWrite(clientContext, chunkId, memoryManager,
                storageServerAddPointer, NUMBER_OF_BYTES_TO_WRITE, testChunkWrite, 0);
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
        ** Now free up all the memory allocated for the test
         */
        storageServerWriteBufferMgr.reset(storageServerAddPointer);
        for (int i = 0; i < NUM_STORAGE_SERVER_WRITE_BUFFERS; i++) {
            ByteBuffer buffer = storageServerWriteBufferMgr.getAndRemove(storageServerAddPointer);

            if (buffer != null) {
                memoryManager.poolMemFree(buffer, storageServerWriteBufferMgr);
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
