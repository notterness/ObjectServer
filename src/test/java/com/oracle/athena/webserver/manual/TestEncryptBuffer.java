package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.http.CasperHttpInfo;
import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.niosockets.NioEventPollThread;
import com.oracle.athena.webserver.operations.EncryptBuffer;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;

public class TestEncryptBuffer {

    private final WebServerFlavor webServerFlavor = WebServerFlavor.INTEGRATION_TESTS;

    private final NioEventPollThread nioEventThread;

    private final RequestContext requestContext;
    private final MemoryManager memoryManager;

    private final EncryptBuffer encryptBuffer;

    private final ByteBufferHttpParser parser;

    TestEncryptBuffer() {
        this.memoryManager = new MemoryManager(WebServerFlavor.INTEGRATION_TESTS);
        this.nioEventThread = new NioEventPollThread(webServerFlavor,0x1001, memoryManager);
        this.nioEventThread.start();

        this.requestContext = new RequestContext(webServerFlavor, memoryManager, nioEventThread);

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
