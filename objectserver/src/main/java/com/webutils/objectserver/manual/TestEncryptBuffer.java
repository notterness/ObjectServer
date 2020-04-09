package com.webutils.objectserver.manual;

import com.webutils.objectserver.operations.EncryptBuffer;
import com.webutils.objectserver.requestcontext.ObjectServerContextPool;
import com.webutils.webserver.http.CasperHttpInfo;
import com.webutils.webserver.http.parser.ByteBufferHttpParser;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;

public class TestEncryptBuffer {

    private static final WebServerFlavor webServerFlavor = WebServerFlavor.INTEGRATION_TESTS;

    private static final int THREAD_BASE_ID = 0x1001;

    private final ObjectServerContextPool contextPool;
    private final RequestContext requestContext;
    private final MemoryManager memoryManager;

    private final EncryptBuffer encryptBuffer;

    public TestEncryptBuffer() {
        this.memoryManager = new MemoryManager(webServerFlavor);
        this.contextPool = new ObjectServerContextPool(webServerFlavor, memoryManager, null);

        this.requestContext = this.contextPool.allocateContextNoCheck(THREAD_BASE_ID);

        /*
        ** encryptInputPointer is setup in the testEncryption() method for EncryptBuffer. So, for
        **   this test passing in null is acceptable.
         */
        encryptBuffer = new EncryptBuffer(requestContext, memoryManager, null, null);
    }

    public void execute() {
        encryptBuffer.testEncryption();

        requestContext.dumpOperations();

        requestContext.reset();

        contextPool.releaseContext(requestContext);
        contextPool.stop(THREAD_BASE_ID);

        memoryManager.verifyMemoryPools("TestEncryptBuffer");
    }
}
