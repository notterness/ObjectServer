package com.oracle.athena.webserver.http;

import com.oracle.athena.webserver.operations.Operation;
import com.oracle.athena.webserver.requestcontext.RequestContext;

public class StorageServerResponseCallback extends HttpResponseCallback {

    private final RequestContext requestContext;
    private final Operation httpResponseReceivedCallback;

    private final int storageServerTcpPort;

    public StorageServerResponseCallback(final RequestContext requestContext, final Operation httpResponseReceivedCb,
                                         final int storageServerTcpPort) {

        this.requestContext = requestContext;
        this.httpResponseReceivedCallback = httpResponseReceivedCb;
        this.storageServerTcpPort = storageServerTcpPort;
    }

    @Override
    public void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {
        System.out.println("httpResponse() status: " + status + " headerCompleted: " + headerCompleted +
                " messageCompleted: " + messageCompleted);

        requestContext.setStorageServerResponse(storageServerTcpPort, status);
        httpResponseReceivedCallback.event();
    }
}
