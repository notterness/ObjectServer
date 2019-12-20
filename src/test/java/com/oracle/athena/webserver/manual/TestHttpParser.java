package com.oracle.athena.webserver.manual;

import com.oracle.athena.webserver.buffermgr.BufferManager;
import com.oracle.athena.webserver.buffermgr.BufferManagerPointer;
import com.oracle.athena.webserver.connectionstate.CasperHttpInfo;
import com.oracle.athena.webserver.connectionstate.Md5Digest;
import com.oracle.athena.webserver.http.parser.ByteBufferHttpParser;
import com.oracle.athena.webserver.memory.MemoryManager;
import com.oracle.athena.webserver.operations.Operation;
import com.oracle.athena.webserver.operations.OperationTypeEnum;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public class TestHttpParser {


    private final RequestContext requestContext;
    private final MemoryManager memoryManager;

    private final ByteBufferHttpParser parser;

    /*
    ** The following are used to feed data into the HTTP Parser
     */
    private BufferManager clientReadBufferMgr;
    private BufferManagerPointer meteringPointer;
    private BufferManagerPointer readPointer;

    TestHttpParser() {

        memoryManager = new MemoryManager(WebServerFlavor.INTEGRATION_TESTS);
        requestContext = new RequestContext(WebServerFlavor.INTEGRATION_TESTS,56, memoryManager);

        CasperHttpInfo casperHttpInfo = new CasperHttpInfo(requestContext);
        parser = new ByteBufferHttpParser(casperHttpInfo);
    }

    boolean execute() {
        boolean testSucceeded = false;

        /*
        ** Need to obtain the ClientReadBufferManager and the meter pointer (to obtain a buffers to put the
        **   request and the payload into) and the read pointer to update that data is present to be
        **   parsed.
         */
        clientReadBufferMgr = requestContext.getClientReadBufferManager();

        readPointer = requestContext.getReadBufferPointer();
        meteringPointer = requestContext.getMeteringPtr();

        /*
        ** To mimic the metering of buffers, need the BufferReadMetering operation to be able to
        **   call its execute() method.
         */
        Operation metering = requestContext.getOperation(OperationTypeEnum.METER_BUFFERS);

        /*
        ** Kick the metering task to allow a buffer to be made
         */
        metering.execute();

        /*
        ** Obtain the ByteBuffer for the HTTP Request and for the content.
        **
        ** NOTE: This is done in a slightly off manner to account for filling the content buffer before filling the
        **   request buffer. Normally, the buffer will be obtained using a peek() operation and then it will be
        **   filled in using either the NIO reader or some other method. Following that, the read pointer will be
        **   updated to allow the consumers to process the data.
        **   In this case, the metering will move forward allowing the poll(readPointer) to obtain a buffer. That will
        **   advance the pointer, but in this test case, it doesn't matter since the http.execute() is called once
        **   at the end.
         */
        ByteBuffer headerBuffer = clientReadBufferMgr.peek(readPointer);
        if (headerBuffer != null) {

            ByteBuffer tempContentBuffer = ByteBuffer.allocate(MemoryManager.MEDIUM_BUFFER_SIZE);
            String md5Digest = buildBufferAndComputeMd5(tempContentBuffer);

            String request = buildRequestString(md5Digest, MemoryManager.MEDIUM_BUFFER_SIZE);
            str_to_bb(headerBuffer, request);

            /*
             ** Now that the HTTP Request is in the buffer, update the read pointer
             */
            clientReadBufferMgr.updateProducerWritePointer(readPointer);


            /*
             ** The HTTP Parser will already have been added to the execute queue in response to the
             **   clientReadBufferMgr.poll(readPointer) operations. Since there was two poll() calls, there
             **   will be two event calls, but it will only be added once.
             **
             ** Run the RequestContext Operation work execute handler. This will cause the work in the HTTP Parser
             **   to actually be done.
             */
            if (requestContext.validateOperationOnQueue(OperationTypeEnum.PARSE_HTTP_BUFFER)) {
                requestContext.performOperationWork();
            }

            /*
             ** Kick the metering task to allow a buffer to be made
             */
            metering.execute();

            ByteBuffer contentBuffer = clientReadBufferMgr.peek(readPointer);
            if (contentBuffer != null) {
                contentBuffer.put(tempContentBuffer.array(), 0, MemoryManager.MEDIUM_BUFFER_SIZE);

                /*
                ** When the HTTP Parsing is complete, the DetermineRequestType operation should be on the execute
                **   queue, so run the performOperationWork() again.
                 */
                if (requestContext.validateOperationOnQueue(OperationTypeEnum.DETERMINE_REQUEST_TYPE)) {
                    requestContext.performOperationWork();
                }

                /*
                ** Following the DetermineRequestType, the SetupV2Put operation should be on the queue.
                 */
                if (requestContext.validateOperationOnQueue(OperationTypeEnum.SETUP_V2_PUT)) {
                    requestContext.performOperationWork();

                    testSucceeded = true;
                }

                /*
                 ** Now that the content data is in the buffer, update the read pointer
                 */
                clientReadBufferMgr.updateProducerWritePointer(readPointer);

                /*
                ** At this point there should be EncryptBuffer on the queue
                 */
                requestContext.validateOperationOnQueue(OperationTypeEnum.ENCRYPT_BUFFER);
            } else {
                System.out.println("Unable to allocate content buffer");
            }
        }

        return testSucceeded;
    }

    private String buildBufferAndComputeMd5(final ByteBuffer contentBuffer) {

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
        for (int i = 0; i < MemoryManager.MEDIUM_BUFFER_SIZE; i = i + 8) {
            contentBuffer.putLong(i, pattern);
            pattern++;
        }

        digest.digestByteBuffer(contentBuffer);
        objectDigestString = digest.getFinalDigest();
        contentBuffer.rewind();

        System.out.println("MD5 Digest String: " + objectDigestString);

        return objectDigestString;
    }

    private String buildRequestString(final String Md5_Digest, final int bytesInContent) {
        return new String("PUT /n/faketenantname" + 
                "/b/bucket-5e1910d0-ea13-11e9-851d-234132e0fb02" +
                "/o/5e223890-ea13-11e9-851d-234132e0fb02 HTTP/1.1\n" +
                "Host: ClientTest-TestHttpParser\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-MD5: " + Md5_Digest + "\n" +
                "Content-Length: " + bytesInContent + "\n\n");
    }

    private void str_to_bb(ByteBuffer out, String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(CharBuffer.wrap(in), out, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
