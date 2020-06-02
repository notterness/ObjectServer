package com.webutils.webserver.manual;

import com.webutils.webserver.http.ClientHttpRequestInfo;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.common.Md5Digest;
import com.webutils.webserver.http.parser.ByteBufferHttpParser;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.niosockets.IoInterface;
import com.webutils.webserver.requestcontext.ClientContextPool;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import com.webutils.webserver.testio.TestEventThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public abstract class WebServerTest {

    private static final Logger LOG = LoggerFactory.getLogger(WebServerTest.class);

    private static final WebServerFlavor webServerFlavor = WebServerFlavor.INTEGRATION_TESTS;

    private static final int threadId = 0x1000;
    private static final int requestId = 56;

    private final ClientContextPool contextPool;

    protected final String testName;

    protected final TestEventThread testEventThread;

    protected final RequestContext requestContext;
    protected final MemoryManager memoryManager;

    /*
    ** The assumption for test that use this base class is that they are going to start by sending an
    **   HTTP request.
     */
    protected final ByteBufferHttpParser parser;


    WebServerTest(final String testName) {

        this.testName = testName;
        this.memoryManager = new MemoryManager(webServerFlavor);

        testEventThread = new TestEventThread(threadId, this);
        testEventThread.start();

        this.contextPool = new ClientContextPool(webServerFlavor, memoryManager, null);
        this.requestContext = this.contextPool.allocateContext(threadId);

        IoInterface connection = testEventThread.allocateConnection(null);
        requestContext.initializeServer(connection, requestId);

        HttpInfo httpRequestInfo = new ClientHttpRequestInfo();
        parser = new ByteBufferHttpParser(httpRequestInfo);
    }

    /*
    ** This is what places data into the passed in ByteBuffer.
     */
    public abstract int read(final ByteBuffer dst);

    /*
    ** This ust be implemented by the child class and will perform the actual work for the test.
     */
    abstract boolean execute();

    /*
    ** The following must be implemented by the child class to build the HTTP Request.
     */
    abstract String buildRequestString(final String testName, final String Md5_Digest, final int bytesInContent);

        /*
    ** Common function to fill in a ByteBuffer with a pattern and then compute the Md5 digest on the
    **   buffer. The Md5 digest is returned as a String.
     */
    protected String buildBufferAndComputeMd5(final ByteBuffer contentBuffer) {

        /*
         ** TODO: Check that the buffer is at least 1k in size
         */

        Md5Digest digest = new Md5Digest();

        /*
         ** Setup the 1kB data transfer here
         */
        String objectDigestString = null;

        // Fill in a pattern
        long pattern = MemoryManager.MEDIUM_BUFFER_SIZE;
        for (int i = 0; i < contentBuffer.limit(); i = i + 8) {
            contentBuffer.putLong(i, pattern);
            pattern++;
        }

        digest.digestByteBuffer(contentBuffer);
        objectDigestString = digest.getFinalDigest();

        LOG.info("MD5 Digest String: " + objectDigestString + " position: " + contentBuffer.position() +
                " limit: " + contentBuffer.limit());

        return objectDigestString;
    }

}
