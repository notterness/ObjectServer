package com.webutils.webserver.manual;

import com.webutils.webserver.http.HttpResponseCallback;
import com.webutils.webserver.http.HttpResponseListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpResponseCompleted extends HttpResponseCallback {

    private static final Logger LOG = LoggerFactory.getLogger(HttpResponseCallback.class);

    private ClientTest clientTest;

    public HttpResponseCompleted(final ClientTest test) {
        clientTest = test;
    }

    @Override
    public void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted) {
        System.out.println("httpResponse() status: " + status + " headerCompleted: " + headerCompleted +
                " messageCompleted: " + messageCompleted);
        LOG.info("httpResponse() status: " + status + " headerCompleted: " + headerCompleted +
                " messageCompleted: " + messageCompleted);

        clientTest.httpResponse(status, headerCompleted, messageCompleted);
    }
}
