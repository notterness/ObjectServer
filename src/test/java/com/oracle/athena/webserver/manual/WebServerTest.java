package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.http.CasperHttpInfo;
import com.oracle.athena.webserver.common.Md5Digest;
import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.niosockets.IoInterface;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.athena.webserver.testio.TestEventThread;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public abstract class WebServerTest {

    private static final Logger LOG = LoggerFactory.getLogger(WebServerTest.class);


    private static final WebServerFlavor webServerFlavor = WebServerFlavor.INTEGRATION_TESTS;

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
        memoryManager = new MemoryManager(webServerFlavor);

        testEventThread = new TestEventThread(webServerFlavor,0x1000, memoryManager,this);
        testEventThread.start();

        requestContext = testEventThread.allocateContext();

        IoInterface connection = testEventThread.allocateConnection(null);
        requestContext.initializeServer(connection, 56);

        CasperHttpInfo casperHttpInfo = new CasperHttpInfo(requestContext);
        parser = new ByteBufferHttpParser(casperHttpInfo);
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

    /*
    ** This copies a String into a ByteBuffer. This is what is used to place the
    **   generated HTTP Request into a buffer to be sent to the WebServer
    **   business logic code.
     */
    protected void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(CharBuffer.wrap(in), out, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        out.flip();

        LOG.info("str_to_bb position: " + out.position() + " limit: " + out.limit());
    }

}
