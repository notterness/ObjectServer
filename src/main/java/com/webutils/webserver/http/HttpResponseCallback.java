package com.webutils.webserver.http;

public abstract class HttpResponseCallback {

    public abstract void httpResponse(final int status, final boolean headerCompleted, final boolean messageCompleted);
}
