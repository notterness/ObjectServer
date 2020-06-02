package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpResponseInfo;
import com.webutils.webserver.http.StorageTierEnum;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/*
 ** ListServers has the following optional headers:
 **   storageTier - List the Storage Servers for a particular storage tier
 **   storage-server-name - This may be a string to match against. For example "server-xyx-*".
 **   limit - The maximum number of items to return
 */
public class ListServersParams extends ObjectParams {

    private static final Logger LOG = LoggerFactory.getLogger(ListServersParams.class);

    private final StorageTierEnum tier;

    private final String serverName;
    private final int limit;

    public ListServersParams(final StorageTierEnum tier, final String serverName, final int limit) {

        super(null, null, null, null);

        this.tier = Objects.requireNonNullElse(tier, StorageTierEnum.INVALID_TIER);
        this.serverName = serverName;
        this.limit = limit;
    }

    /*
     ** This builds the ListServers GET method headers. The following headers and if they are required:
     **
     **   Host (required) - Who is sending the request.
     **   opc-client-request-id (not required) - A unique identifier for this request provided by the client to allow
     **     then to track their requests.
     **   Content-Length (required) - Always set to 0
     **
     ** The following are optional and are used to narrow the search:
     **   storageTier - List the servers that provide a particular storage tier. If the tier is set to INVALID_TIER
     **     do not send this header.
     **   storage-server-name - This may be a string to match against. For example "server-xyx-*". This will return all
     **     the servers associated with the matching storage server(s). If it is set to null, do not send this header.
     **   limit - The maximum number of items to return (if set to 0, do not send the header)
     */
    public String constructRequest() {
        String initialContent = "GET /l/servers HTTP/1.1\n";

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

        if (tier != StorageTierEnum.INVALID_TIER) {
            request += "storageTier: " + tier.toString() + "\n";
        }

        if (serverName != null) {
            request += "storage-server-name: " + serverName + "\n";
        }

        if (limit != 0) {
            request += "limit: " + limit + "\n";
        }

        finalContent += "Content-Length: 0\n\n";

        request += finalContent;

        return request;
    }

    /*
     ** This displays the results from the ListServers method.
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
        if (httpInfo.getResponseStatus() == HttpStatus.OK_200) {
            System.out.println("Status: 200");
            String opcClientRequestId = httpInfo.getOpcClientRequestId();
            if (opcClientRequestId != null) {
                System.out.println("opc-client-request-id: " + opcClientRequestId);
            }

            String opcRequestId = httpInfo.getOpcRequestId();
            if (opcRequestId != null) {
                System.out.println("opc-request-id: " + opcRequestId);
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
