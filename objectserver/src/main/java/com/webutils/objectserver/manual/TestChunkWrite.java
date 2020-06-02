package com.webutils.objectserver.manual;

import com.webutils.objectserver.operations.SetupChunkWrite;
import com.webutils.objectserver.requestcontext.ObjectServerContextPool;
import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.mysql.ServersDb;
import com.webutils.webserver.niosockets.EventPollThread;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.niosockets.NioTestClient;
import com.webutils.webserver.operations.OperationTypeEnum;
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

    /*
     ** A unique identifier for this Operation so it can be tracked.
     */
    private final OperationTypeEnum operationType = OperationTypeEnum.TEST_CHUNK_WRITE;

    private static final WebServerFlavor flavor = WebServerFlavor.INTEGRATION_TESTS;

    private final int NUM_STORAGE_SERVER_WRITE_BUFFERS = 10;
    private final int NUMBER_OF_BYTES_TO_WRITE = MemoryManager.XFER_BUFFER_SIZE * 10;

    private final InetAddress storageServerAddr;
    private final int storageServerTcpPort;

    private final AtomicInteger runningTestCount;

    private final String errorInjectString;

    private final MemoryManager objServerMemMgr;
    private final ObjectServerContextPool objReqContextPool;

    protected final NioTestClient testClient;
    protected final EventPollThread eventThread;
    private final int eventThreadId;

    private boolean statusSignalSent;
    private final Object writeDone;


    /*
    ** The addr and storageServerTcpPort are used to identify the Storage Server the ChunkWrite will be directed to.
     */
    public TestChunkWrite(final InetAddress addr, final int storageServerTcpPort, AtomicInteger testCount,
                          final ServerIdentifierTableMgr serverTableMgr, final String errorInjectString) {

        this.storageServerAddr = addr;
        this.storageServerTcpPort = storageServerTcpPort;
        this.runningTestCount = testCount;
        this.errorInjectString = errorInjectString;

        /*
         ** The testClient is responsible for providing the threads the Operation(s) will run on and the
         **   NIO Socket handling.
         */
        this.objServerMemMgr = new MemoryManager(flavor);
        this.objReqContextPool = new ObjectServerContextPool(flavor, objServerMemMgr, serverTableMgr);

        this.testClient = new NioTestClient(objReqContextPool);
        this.testClient.start();

        this.eventThread = testClient.getEventThread();
        this.eventThreadId = this.eventThread.getEventPollThreadBaseId();

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

    public void execute() {
        /*
         ** Allocate a RequestContext
         */
        RequestContext clientContext = objReqContextPool.allocateContext(eventThreadId);

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
            ByteBuffer writeBuffer = objServerMemMgr.poolMemAlloc(MemoryManager.XFER_BUFFER_SIZE, storageServerWriteBufferMgr,
                    operationType);

            /*
            ** Write a known pattern into the buffers
             */
            for (int j = 0; j < MemoryManager.XFER_BUFFER_SIZE; j += 4) {
                writeBuffer.putInt(j, fillPattern);
                fillPattern++;
            }
            storageServerWriteBufferMgr.offer(storageServerAddPointer, writeBuffer);
        }

        SetupChunkWrite setupChunkWrite = new SetupChunkWrite(clientContext, chunkId, objServerMemMgr,
                storageServerAddPointer, NUMBER_OF_BYTES_TO_WRITE, testChunkWrite, 0, errorInjectString);
        setupChunkWrite.initialize();

        /*
         ** Start the process of setting up and writing a chunk to the Storage Server
         */
        setupChunkWrite.event();

        /*
         ** Now wait for the status to be received and then it can verified with the expected value
         */
        if (!waitForStatus()) {
            LOG.error("Status was not received and wait timed out");
        }

        /*
        ** Now free up all the memory allocated for the test
         */
        storageServerWriteBufferMgr.reset(storageServerAddPointer);
        for (int i = 0; i < NUM_STORAGE_SERVER_WRITE_BUFFERS; i++) {
            ByteBuffer buffer = storageServerWriteBufferMgr.getAndRemove(storageServerAddPointer);

            if (buffer != null) {
                objServerMemMgr.poolMemFree(buffer, storageServerWriteBufferMgr);
            } else {
                LOG.info("TestChunkWrite storageServerAddPointer index: " + storageServerAddPointer.getCurrIndex());
            }
        }
        storageServerWriteBufferMgr.unregister(storageServerAddPointer);
        storageServerWriteBufferMgr.reset();

        eventThread.releaseConnection(connection);

        clientContext.reset();
        objReqContextPool.releaseContext(clientContext);

        /*
        ** Verify all the ByteBuffer(s) were returned to the MemoryManager
         */
        String caller = "TestChunkWrite-" + eventThreadId;
        objServerMemMgr.verifyMemoryPools(caller);

        testClient.stop();

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
