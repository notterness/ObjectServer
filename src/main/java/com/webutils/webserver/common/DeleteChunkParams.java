package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
** THis is used by the Chunk Manager Service and the Client Test to send the DeleteChunk call to the Storage Server
 */
public abstract class DeleteChunkParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteChunkParams.class);

    private ServerIdentifier server;

    public DeleteChunkParams(final ServerIdentifier server) {

        super(null, null, null, null);

        this.server = server;
    }

    /*
     ** This builds the StorageServerDeleteChunk DELETE method headers. The following lists the headers and if they are required:
     **
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   object-chunk-number (required) -
     **   chunk-lba (required) -
     **   chunk-location (required) -
     **   Content-Length (required) - Must be set to 0
     */
    public String constructRequest() {
        String initialContent = "DELETE /o/StorageServer HTTP/1.1\n";

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

        request += HttpInfo.CHUNK_LBA + ": " + server.getChunkLBA() + "\n" +
                HttpInfo.CHUNK_ID + ": " + server.getChunkId() + "\n" +
                HttpInfo.CHUNK_LOCATION + ": " + server.getChunkLocation() + "\n" +
                HttpInfo.CONTENT_LENGTH + ": 0\n\n";

        return request;
    }

    /*
     ** This displays the results from the StorageServerDeleteChunks method.
     */
    public abstract void outputResults(final HttpResponseInfo httpInfo);

}
