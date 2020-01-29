package com.webutils.webserver.manual;

import com.webutils.webserver.buffermgr.BufferManager;
import com.webutils.webserver.buffermgr.BufferManagerPointer;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.operations.OperationTypeEnum;
import com.webutils.webserver.testio.TestIoGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class TestHttpParser extends WebServerTest {

    private static final Logger LOG = LoggerFactory.getLogger(TestHttpParser.class);

    /*
    ** The following are used to feed data into the HTTP Parser
     */
    private BufferManager clientReadBufferMgr;
    private BufferManagerPointer readPointer;

    private ByteBuffer tempContentBuffer;

    private int timesCalled;

    TestHttpParser(final String testName) {
        super(testName);

        this.timesCalled = 0;
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

        /*
        ** To mimic the metering of buffers, need the BufferReadMetering operation to be able to
        **   call its execute() method.
         */
        Operation metering = requestContext.getOperation(OperationTypeEnum.METER_READ_BUFFERS);

        /*
        ** Kick the metering task to allow a buffer to be added (made available to write data to) in the
        **   clientReadBufferManager
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
        if (requestContext.validateOperationOnQueue(OperationTypeEnum.READ_BUFFER)) {
            requestContext.performOperationWork();
        }

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
         * At this point there should be EncryptBuffer on the queue
         */
        requestContext.validateOperationOnQueue(OperationTypeEnum.ENCRYPT_BUFFER);

        requestContext.dumpOperations();

        /*
        ** The cleanupRequest() and stop() must be called to release the resources back so there are no outstanding
        **   references that prevent the program from cleanly shutting down.
         */
        super.requestContext.cleanupServerRequest();
        super.testEventThread.stop();

        return testSucceeded;
    }

    /*
    ** This is passed a ByteBuffer by the TestIoGenerator and it will fill in the buffer with information
    **   generated.
     */
    public int read(final ByteBuffer dst) {
        if (timesCalled == 0) {
            /*
            ** Fill in the HTTP Request information
             */
            tempContentBuffer = ByteBuffer.allocate(MemoryManager.XFER_BUFFER_SIZE);
            String md5Digest = buildBufferAndComputeMd5(tempContentBuffer);

            String request = buildRequestString(super.testName, md5Digest, tempContentBuffer.limit());
            str_to_bb(dst, request);

            timesCalled++;
        } else if (timesCalled == 1){
            /*
            ** Fill in the PUT object content data
             */
            dst.put(tempContentBuffer.array(), 0, tempContentBuffer.limit());
            dst.flip();

            LOG.info("read(1) position: " + dst.position() + " limit: " + dst.limit());

            tempContentBuffer = null;

            timesCalled++;
        } else {
            return -1;
        }

        return dst.position();
    }

    String buildRequestString(final String testName, final String Md5_Digest, final int bytesInContent) {
        return new String("PUT /n/faketenantname" + 
                "/b/bucket-5e1910d0-ea13-11e9-851d-234132e0fb02" +
                "/o/5e223890-ea13-11e9-851d-234132e0fb02 HTTP/1.1\n" +
                "Host: ClientTest-" + testName + "\n" +
                "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: Rested/2009 CFNetwork/978.0.7 Darwin/18.7.0 (x86_64)\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n" +
                "Content-MD5: " + Md5_Digest + "\n" +
                "Content-Length: " + bytesInContent + "\n\n");
    }

}
