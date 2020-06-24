package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpResponseInfo;
import org.eclipse.jetty.http.HttpStatus;

public class HealthCheckParams extends ObjectParams {

    private final boolean enableService;
    private final boolean disableService;

    private final String accessToken;

    public HealthCheckParams(final boolean enableService, final boolean disableService, final String accessToken) {

        super(null, null, null, null);

        this.enableService = enableService;
        this.disableService = disableService;

        this.accessToken = accessToken;
    }

    /*
     ** This builds the HealthCheck request headers. The following headers and if they are required:
     **
     */
    public String constructRequest() {
        String initialContent = "GET /health HTTP/1.1\n";

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

        /*
        ** Enable Service overrides Disable Service. Only add the header if either one is set to true, otherwise do not
        **   send the header.
         */
        if (enableService) {
            request += HttpInfo.ENABLE_SERVICE + ": true\n";
        } else if (disableService) {
            request += HttpInfo.DISABLE_SERVICE + ": true\n";
        }

        if (accessToken != null) {
            request += HttpInfo.ACCESS_TOKEN + ": " + accessToken + "\n";
        }

        finalContent += HttpInfo.CONTENT_LENGTH + ": 0\n\n";

        request += finalContent;

        return request;
    }

    /*
     ** This displays the results from the HealthCheck method.
     **
     ** TODO: Allow the results to be dumped to a file and possibly allow a format that allows for easier parsing by
     **   the client.
     */
    public void outputResults(final HttpResponseInfo httpInfo) {
        /*
         ** If the status is good (HealthCheck returns 200 if successful), then display the following:
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
            System.out.println(HttpInfo.OPC_REQUEST_ID + ": " + opcRequestId);
        }

        if (httpInfo.getResponseStatus() == HttpStatus.METHOD_NOT_ALLOWED_405) {
            System.out.println("Status: " + httpInfo.getResponseStatus());
            String allowableMethods = httpInfo.getAllowableMethods();
            if (allowableMethods != null) {
                System.out.println("Allowed Methods: " + allowableMethods);
            }
        } else if (status != HttpStatus.OK_200){
            String responseBody = httpInfo.getResponseBody();
            if (responseBody != null) {
                System.out.println(responseBody);
            }
        }
    }


}
