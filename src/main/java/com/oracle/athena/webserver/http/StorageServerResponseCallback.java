package com.oracle.athena.webserver.http;

import com.oracle.athena.webserver.operations.Operation;
import com.oracle.athena.webserver.requestcontext.RequestContext;
import com.oracle.athena.webserver.requestcontext.ServerIdentifier;

public class StorageServerResponseCallback extends HttpResponseCallback {

    private final RequestContext requestContext;
    private final Operation httpResponseReceivedCallback;

    private final ServerIdentifier serverIdentifier;

    public StorageServerResponseCallback(final RequestContext requestContext, final Operation httpResponseReceivedCb,
                                         final ServerIdentifier serverIdentifier) {

        this.requestContext = requestContext;
        this.httpResponseReceivedCallback = httpResponseReceivedCb;
        this.serverIdentifier = serverIdentifier;
    }

    @Override
    public void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {
        System.out.println("httpResponse() status: " + status + " headerCompleted: " + headerCompleted +
                " messageCompleted: " + messageCompleted);

        requestContext.setStorageServerResponse(serverIdentifier, status);
        httpResponseReceivedCallback.event();
    }
}
