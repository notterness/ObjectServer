package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpResponseCallback;
import com.webutils.webserver.operations.Operation;
import com.webutils.webserver.requestcontext.RequestContext;
import com.webutils.webserver.requestcontext.ServerIdentifier;

public class ServiceResponseCallback extends HttpResponseCallback {

    private final RequestContext requestContext;
    private final Operation httpResponseReceivedCallback;

    private final ServerIdentifier serverIdentifier;

    public ServiceResponseCallback(final RequestContext requestContext, final Operation httpResponseReceivedCb,
                                         final ServerIdentifier serverIdentifier) {

        this.requestContext = requestContext;
        this.httpResponseReceivedCallback = httpResponseReceivedCb;
        this.serverIdentifier = serverIdentifier;
    }

    @Override
    public void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {
        /*
         ** Uncomment out the following for additional debug if needed.
         */
        System.out.println("httpResponse() status: " + status + " headerCompleted: " + headerCompleted +
                " messageCompleted: " + messageCompleted);

        requestContext.setStorageServerResponse(serverIdentifier, status);
        httpResponseReceivedCallback.event();
    }

}
