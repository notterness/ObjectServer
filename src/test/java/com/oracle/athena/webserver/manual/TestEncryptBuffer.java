package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.connectionstate.CasperHttpInfo;
import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.NioEventPollThread;
import com.oracle.athena.webserver.operations.EncryptBuffer;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;

public class TestEncryptBuffer {

    private final NioEventPollThread nioEventThread;

    private final RequestContext requestContext;
    private final MemoryManager memoryManager;

    private final EncryptBuffer encryptBuffer;

    private final ByteBufferHttpParser parser;

    TestEncryptBuffer() {
        nioEventThread = new NioEventPollThread(0x1001);

        memoryManager = new MemoryManager(WebServerFlavor.INTEGRATION_TESTS);
        requestContext = new RequestContext(WebServerFlavor.INTEGRATION_TESTS, 55, memoryManager, nioEventThread);

        CasperHttpInfo casperHttpInfo = new CasperHttpInfo(requestContext);

        parser = new ByteBufferHttpParser(casperHttpInfo);

        encryptBuffer = new EncryptBuffer(requestContext, null);
    }

    void execute() {
        encryptBuffer.testEncryption();

        requestContext.dumpOperations();
    }
}
