package com.webutils.webserver.common;

import com.webutils.webserver.http.*;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


/*
 ** ListChunks has the following optional headers:
 **   storageTier - List the chunks for a particular storage tier
 **   storage-server-name - This may be a string to match against. For example "server-xyx-*". This will return all
 **     the chunks associated with the matching storage server(s).
 **   chunk-status - This can be AVAILABLE, ALLOCATED or DELETED.
 **   limit - The maximum number of items to return
 */
public class ListChunksParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(ListChunksParams.class);

    private final StorageTierEnum tier;

    private final String serverName;
    private final ChunkStatusEnum chunkStatus;
    private final int limit;

    public ListChunksParams(final StorageTierEnum tier, final String serverName, final ChunkStatusEnum status, final int limit) {

        super(null, null, null, null);

        this.tier = Objects.requireNonNullElse(tier, StorageTierEnum.INVALID_TIER);

        this.serverName = serverName;
        this.chunkStatus = Objects.requireNonNullElse(status, ChunkStatusEnum.INVALID_CHUNK_STATUS);

        this.limit = limit;
    }

    /*
     ** This builds the ListChunks GET method headers. The following headers and if they are required:
     **
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   Content-Length (required) - Always set to 0
     **
     ** The following are optional and are used to narrow the search:
     **   storageTier - List the chunks for a particular storage tier. If the tier is set to INVALID_TIER do not send
     **     this header.
     **   storage-server-name - This may be a string to match against. For example "server-xyx-*". This will return all
     **     the chunks associated with the matching storage server(s). If it is set to null, do not send this header.
     **   chunk-status - This can be AVAILABLE, ALLOCATED or DELETED. If it is set to INVALID_CHUNK_STATUS, do not
     **     send this header.
     **   limit - The maximum number of items to return (if set to 0, do not send the header)
     */
    public String constructRequest() {
        String initialContent = "GET /l/chunks HTTP/1.1\n";

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

        if (tier != StorageTierEnum.INVALID_TIER) {
            request += HttpRequestInfo.STORAGE_TIER_HEADER + ": " + tier.toString() + "\n";
        }

        if (serverName != null) {
            request += ContentParser.SERVICE_NAME + ": " + serverName + "\n";
        }

        if (chunkStatus != ChunkStatusEnum.INVALID_CHUNK_STATUS) {
            request += HttpRequestInfo.CHUNK_STATUS_HEADER + ": " + chunkStatus.toString() + "\n";
        }

        if (limit != 0) {
            request += "limit: " + limit + "\n";
        }

        finalContent += HttpInfo.CONTENT_LENGTH + ": 0\n\n";

        request += finalContent;

        return request;
    }

    /*
     ** This displays the results from the ListChunks method.
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good (ListChunks returns 200 if successful), then display the following:
         **   opc-client-request-id
         **   opc-request-id
         */
        int status = httpInfo.getResponseStatus();
        System.out.println("Status: " + status);
        String opcClientRequestId = httpInfo.getOpcClientRequestId();
        if (opcClientRequestId != null) {
            System.out.println(HttpInfo.CLIENT_OPC_REQUEST_ID + ": " + opcClientRequestId);
        }

        String opcRequestId = httpInfo.getOpcRequestId();
        if (opcRequestId != null) {
            System.out.println(HttpInfo.OPC_REQUEST_ID + "opc-request-id: " + opcRequestId);
        }

        if (httpInfo.getResponseStatus() == HttpStatus.METHOD_NOT_ALLOWED_405) {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String allowableMethods = httpInfo.getAllowableMethods();
            if (allowableMethods != null) {
                System.out.println("Allowed Methods: " + allowableMethods);
            }
        } else {
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }

}
