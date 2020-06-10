package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageServerChunkDeleteParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(StorageServerChunkDeleteParams.class);

    private ServerIdentifier server;

    public StorageServerChunkDeleteParams(final ServerIdentifier server) {

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
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good (StorageServerDeleteChunks returns 200 if successful), then display the following:
         **   opc-client-request-id
         **   opc-request-id
         */
        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            System.out.println("Status: 200");
            String opcClientRequestId = httpInfo.getOpcClientRequestId();
            if (opcClientRequestId != null) {
                System.out.println(HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId);
            }

            String opcRequestId = httpInfo.getOpcRequestId();
            if (opcRequestId != null) {
                System.out.println(HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId);
            }

            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }

        } else if (httpInfo.getResponseStatus() == HttpStatus.METHOD_NOT_ALLOWED_405) {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String allowableMethods = httpInfo.getAllowableMethods();
            if (allowableMethods != null) {
                System.out.println("Allowed Methods: " + allowableMethods);
            }
        } else {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

}
