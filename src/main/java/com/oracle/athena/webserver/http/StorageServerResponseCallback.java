package com.oracle.athena.webserver.http;

import com.oracle.athena.webserver.operations.Operation;

public class StorageServerResponseCallback extends HttpResponseCallback {

    private final Operation httpResponseReceivedCallback;

    public StorageServerResponseCallback(final Operation httpResponseReceivedCb) {

        this.httpResponseReceivedCallback = httpResponseReceivedCb;
    }

    @Override
    public void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {
        System.out.println("httpResponse() status: " + status + " headerCompleted: " + headerCompleted +
                " messageCompleted: " + messageCompleted);

        httpResponseReceivedCallback.event();
    }
}
