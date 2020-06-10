package com.webutils.webserver.common;

import com.webutils.webserver.http.ContentParser;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public abstract class DeleteChunksParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteChunksParams.class);

    private String sha256Digest;
    private List<ServerIdentifier> servers;

    public DeleteChunksParams(final List<ServerIdentifier> servers) {

        super(null, null, null, null);

        LOG.info("server size() " + servers.size());
        this.servers = servers;
    }

    /*
     ** This builds the DeleteChunks DELETE method headers. The following lists the headers and if they are required:
     **
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   x-content-sha256 - A sha256 digest of the content.
     **   Content-Length (required) - The size of the content that is used to describe the chunks that are being requested.
     */
    public String constructRequest() {
        String initialContent = "DELETE / HTTP/1.1\n";

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
            request += "opc-client-request-id: " + opcClientRequestId + "\n";
        }

        finalContent += "x-content-sha256: " + sha256Digest + "\n" +
                "Content-Length: " + contentSizeInBytes + "\n\n" +
                contentStr;

        request += finalContent;

        return request;
    }

    /*
    ** The request content for the DeleteChunks request to the ChunkMgr service looks like the following:
    **
    **   {
    **      "chunk-chunkIndex":
    **       {
    **         "storage-server-name": "  server.getServerName()  "
    **          "chunk-etag": " server.getChunkUID() "
    **       }
     */
    private String buildContent() {

        StringBuilder body = new StringBuilder();
        int chunkIndex = 0;

        body.append("{\n");
        for (ServerIdentifier server : servers) {
            body.append("  \"chunk-" + chunkIndex + "\":\n");
            body.append("    {\n");
            body.append("       \"" + ContentParser.SERVER_NAME + "\": \"" + server.getServerName() + "\"\n");
            body.append("       \"" + ContentParser.CHUNK_UID + "\": \"" + server.getChunkUID() + "\"\n");
            body.append("    }\n");

            chunkIndex++;
        }
        body.append("}\n");

        String contentString = body.toString();

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
     ** This displays the results from the DeleteChunks method. It is implemented differently for the Object Server
     **   and the Client Test.
     */
    public abstract void outputResults(final HttpResponseInfo httpInfo);
}
