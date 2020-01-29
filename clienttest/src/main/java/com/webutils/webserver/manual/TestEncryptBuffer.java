package com.webutils.webserver.manual;

import com.webutils.webserver.http.CasperHttpInfo;
import com.webutils.webserver.http.parser.ByteBufferHttpParser;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.NioEventPollThread;
import com.webutils.webserver.operations.EncryptBuffer;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;

public class TestEncryptBuffer {

    private final WebServerFlavor webServerFlavor = WebServerFlavor.INTEGRATION_TESTS;

    private final NioEventPollThread nioEventThread;

    private final RequestContext requestContext;
    private final MemoryManager memoryManager;

    private final EncryptBuffer encryptBuffer;

    private final ByteBufferHttpParser parser;

    TestEncryptBuffer() {
        this.memoryManager = new MemoryManager(WebServerFlavor.INTEGRATION_TESTS);
        this.nioEventThread = new NioEventPollThread(webServerFlavor, null,0x1001, memoryManager, null);
        this.nioEventThread.start();

        this.requestContext = new RequestContext(webServerFlavor, memoryManager, nioEventThread, null);

        CasperHttpInfo casperHttpInfo = new CasperHttpInfo(requestContext);
        this.parser = new ByteBufferHttpParser(casperHttpInfo);

        /*
        ** encryptInputPointer is setup in the testEncryption() method for EncryptBuffer. So, for
        **   this test passing in null is acceptable.
         */
        encryptBuffer = new EncryptBuffer(requestContext, memoryManager, null, null);
    }

    void execute() {
        encryptBuffer.testEncryption();

        requestContext.dumpOperations();

        requestContext.reset();

        nioEventThread.stop();
    }
}
