package com.oracle.athena.webserver.http;

public abstract class HttpResponseCallback {

    public abstract void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted);
}
