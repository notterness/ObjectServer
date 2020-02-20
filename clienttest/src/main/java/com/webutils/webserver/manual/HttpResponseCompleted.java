package com.webutils.webserver.manual;

import com.webutils.webserver.http.HttpResponseCallback;

public class HttpResponseCompleted extends HttpResponseCallback {

    private ClientTest clientTest;

    public HttpResponseCompleted(final ClientTest test) {
        clientTest = test;
    }

    @Override
    public void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {
        System.out.println("httpResponse() status: " + status + " headerCompleted: " + headerCompleted +
                " messageCompleted: " + messageCompleted);

        clientTest.httpResponse(status, headerCompleted, messageCompleted);
    }
}