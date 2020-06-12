package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.http.StorageTierEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

public abstract class AllocateChunksParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(AllocateChunksParams.class);

    private final StorageTierEnum tier;

    private String sha256Digest;
    private final int objectChunkNumber;

    public AllocateChunksParams(final StorageTierEnum tier, final int objectChunkNumber) {

        super(null, null, null, null);

        this.tier = tier;
        this.objectChunkNumber = objectChunkNumber;
    }

    /*
     ** This builds the AllocateChunks GET method headers. The following headers and if they are required:
     **
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   x-content-sha256 - A sha256 digest of the content.
     **   Content-Length (required) - The size of the content that is used to describe the chunks that are being requested.
     */
    public String constructRequest() {
        String initialContent = "GET / HTTP/1.1\n";

        String contentStr = buildContent();
        int contentSizeInBytes = contentStr.length();

        String finalContent = "Content-Type: application/json\n" +
                "Connection: keep-alive\n" +
                "Accept: */*\n" +
                "User-Agent: ClientRequest/0.0.1\n" +
                "Accept-Language: en-us\n" +
                "Accept-Encoding: gzip, deflate\n";

        String request;
        if (hostName != null) {
            request = initialContent + "Host: " + hostName + "\n";
        } else {
            request = initialContent + "Host: ClientTest\n";
        }

        if (opcClientRequestId != null) {
            request += HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId + "\n";
        }

        finalContent += HttpInfo.CONTENT_SHA256 + ": " + sha256Digest + "\n" +
                HttpInfo.CONTENT_LENGTH + ": " + contentSizeInBytes + "\n\n" +
                contentStr;

        request += finalContent;

        return request;
    }

    private String buildContent() {
        String contentString =
                "{\n" +
                "  \"" + HttpInfo.CHUNK_NUMBER + "\": \"" + objectChunkNumber + "\",\n" +
                "  \"" + HttpRequestInfo.STORAGE_TIER_HEADER + "\": \"" + tier.toString() + "\",\n" +
                "}\n";

        Sha256Digest digest = new Sha256Digest();

        /*
         ** The Sha-256 digest works on ByteBuffer, so the contentString needs to be pushed into a ByteBuffer and
         **   then the Sha-256 digest run on it.
         */
        ByteBuffer tmpBuffer = ByteBuffer.allocate(contentString.length());
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(CharBuffer.wrap(contentString), tmpBuffer, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        /*
         ** Now compute the Sha-256 digest
         */
        tmpBuffer.flip();
        digest.digestByteBuffer(tmpBuffer);
        sha256Digest = digest.getFinalDigest();

        return contentString;
    }

    /*
     ** This displays the results from the AllocateChunks method.
     **
     ** This is an abstract method to allow the user of this class to decide what they want to display when the
     **   AllocateChunks method request completes.
     */
    public abstract void outputResults(final HttpResponseInfo httpInfo);

}
