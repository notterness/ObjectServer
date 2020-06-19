package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.memory.MemoryManager;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

public abstract class ObjectParamsWithData extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectParamsWithData.class);

    protected final MemoryManager memoryManager;

    protected final List<ByteBuffer> requestData;
    private int currIndex;

    private final boolean validMd5;

    private final Md5Digest digest;
    protected String computedMd5Digest;

    protected final String accessToken;

    public ObjectParamsWithData(final MemoryManager memoryManager, final String namespace, final String bucket,
                                final String object, final boolean validMd5, final String accessToken) {

        super(namespace, bucket, object, null);

        this.memoryManager = memoryManager;

        this.requestData = new LinkedList<>();
        this.currIndex = 0;

        /*
        ** This is used as a test parameter to modify the computed Md5 digest to make it invalid.
         */
        this.validMd5 = validMd5;

        this.digest = new Md5Digest();

        /*
        ** Used to validate the user for the request
         */
        this.accessToken = accessToken;
    }

    /*
    ** poll() advances the read pointer after the buffer is returned.
    **
    ** NOTE: Is there a reason to make this thread safe?
     */
    public ByteBuffer poll() {
        ByteBuffer buffer;
        if (currIndex < requestData.size()) {
            buffer = requestData.get(currIndex);
            currIndex++;
        } else {
            buffer = null;
        }

        return buffer;
    }

    /*
    ** peek() returns a buffer if one exists, but does not update the read pointer.
    **
    ** NOTE: Is there a reason to make this thread safe?
     */
    public ByteBuffer peek() {
        ByteBuffer buffer;
        if (currIndex < requestData.size()) {
            buffer = requestData.get(currIndex);
        } else {
            buffer = null;
        }

        return buffer;
    }

    /*
    ** Return all the ByteBuffers to the Memory Manager and then clear out the linked list.
     */
    public void clear() {
        for (ByteBuffer buffer: requestData) {
            memoryManager.poolMemFree(buffer, null);
        }

        requestData.clear();
    }

    public String getMd5Digest() {
        return computedMd5Digest;
    }

    /*
    ** Compute the Md5 digest for the buffers provided in the requestData linked list
     */
    protected void computeMd5Digest() {
        for (ByteBuffer dataBuffer: requestData) {
            digest.digestByteBuffer(dataBuffer);
            dataBuffer.rewind();
        }

        /*
        ** Check if the Md5 checksum should be corrupted
         */
        if (!validMd5) {
            String corruptStr = "This will corrupt the Md5 Digest";
            ByteBuffer tmpBuffer = ByteBuffer.allocate(corruptStr.length());
            HttpInfo.str_to_bb(tmpBuffer, corruptStr);

            /*
             ** Need to flip() the buffer so that the limit() is set to the end of where the HTTP Request is
             **   and the position() reset to 0.
             */
            tmpBuffer.flip();
            digest.digestByteBuffer(tmpBuffer);
        }

        computedMd5Digest = digest.getFinalDigest();
        System.out.println("MD5 Digest String: " + computedMd5Digest + " validMd5: " + validMd5);
    }

    public abstract void constructRequestData();

    /*
     ** This displays the results from the PutObject method.
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good, then display the following:
         **   opc-client-request-id
         **   opc-request-id
         **   opc-content-md5
         **   ETag
         **   last-modified
         **   version-id
         */
        int status = httpInfo.getResponseStatus();
        System.out.println("Status: " + status);
        String opcClientRequestId = httpInfo.getOpcClientRequestId();
        if (opcClientRequestId != null) {
            System.out.println(HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId);
        }

        String opcRequestId = httpInfo.getOpcRequestId();
        if (opcRequestId != null) {
            System.out.println(HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId);
        }

        if (status == HttpStatus.OK_200) {
            String contentMd5 = httpInfo.getResponseContentMd5();
            if (contentMd5 != null) {
                System.out.println(HttpResponseInfo.OPC_CONTENT_MD5 + ": " + contentMd5);
            }

            String etag = httpInfo.getResponseEtag();
            if (etag != null) {
                System.out.println(HttpResponseInfo.RESPONSE_HEADER_ETAG + ": " + etag);
            }

            String lastModified = httpInfo.getResponseLastModified();
            if (lastModified != null) {
                System.out.println(HttpResponseInfo.RESPONSE_LAST_MODIFIED + ": " + lastModified);
            }

            String versionId = httpInfo.getResponseVersionId();
            if (versionId != null) {
                System.out.println(HttpResponseInfo.RESPONSE_VERSION_ID + ": " + versionId);
            }
        } else {
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

    /*
     ** This validates that the file the client wants to upload is present and has a size greater than 0.
     */
    public long setObjectSizeInBytes() {
        objectSizeInBytes = 0;
        if (objectFilePath != null) {
            File inFile = new File(objectFilePath);
            try {
                FileChannel readFileChannel = new FileInputStream(inFile).getChannel();

                try {
                    objectSizeInBytes = readFileChannel.size();
                } catch (IOException io_ex) {
                    LOG.error("Unable to obtain file length - " + objectFilePath + " ex:" + io_ex.getMessage());
                }
            } catch (FileNotFoundException ex) {
                LOG.info("setObjectSizeInBytes()) file not found: " + objectFilePath + " ex:" + ex.getMessage());
            }
        }

        LOG.info("setObjectSizeInBytes() bytesToReadFromFile: " + objectSizeInBytes);

        return objectSizeInBytes;
    }

    public void setObjectSizeInBytes(final long sizeInBytes) {
        objectSizeInBytes = sizeInBytes;
    }


}
