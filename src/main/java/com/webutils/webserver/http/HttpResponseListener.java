package com.webutils.webserver.http;

import org.eclipse.jetty.http.*;
import org.eclipse.jetty.util.BufferUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpResponseListener implements HttpParser.ResponseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpResponseListener.class);

    private final HttpResponseInfo httpInfo;
    private final HttpResponseCallback httpResponseCb;

    public HttpResponseListener(final HttpResponseInfo httpInfo, final HttpResponseCallback callback) {

        this.httpInfo = httpInfo;
        this.httpResponseCb = callback;
    }

    @Override
    public boolean content(ByteBuffer ref) {

        return false;
    }

    @Override
    public void parsedHeader(HttpField field) {

        //LOG.info("parseHeader(resp) _hdr: " + field.getName() + " _val: " + field.getValue());

        httpInfo.addHeaderValue(field);
    }

    @Override
    public boolean headerComplete() {
        LOG.info("headerComplete(resp)");
        httpInfo.setHeaderComplete();
        return false;
    }

    @Override
    public void parsedTrailer(HttpField field) {
        LOG.info("parsedTrailer(resp) " + field.getValue());
    }

    @Override
    public boolean contentComplete() {
        LOG.info("contentComplete(resp)");

        httpInfo.setContentComplete();
        return false;
    }

    @Override
    public boolean messageComplete() {
        httpInfo.setMessageComplete();

        if (httpResponseCb != null) {
            LOG.info("messageComplete(resp)");

            httpResponseCb.httpResponse(httpInfo.getResponseStatus(), httpInfo.getHeaderComplete(), true);
        } else {
            LOG.info("messageComplete(resp) httpResponseCb null");
        }
        return true;
    }

    @Override
    public void badMessage(BadMessageException failure) {
        String reason = failure.getReason();

        LOG.info("badMessage(resp) reason: " + reason);
        httpInfo.httpHeaderError(failure);
    }

    @Override
    public boolean startResponse(HttpVersion version, int status, String reason) {
        LOG.info("StartResponse(resp) version: " + version + " status: " + status +
                " reason: " + reason);
        String methodOrVersion = version.asString();
        String uriOrStatus = Integer.toString(status);
        httpInfo.setResponseStatus(status);
        return false;
    }

    @Override
    public void earlyEOF() {
        LOG.info("earlyEOF(resp)");
    }

    @Override
    public int getHeaderCacheSize() {
        return 4096;
    }

    /*
    @Override
    public void onComplianceViolation(HttpCompliance compliance, HttpComplianceSection violation, String reason)
    {
        LOG.info("onComplianceViolation()" + reason);
        _complianceViolation.add(violation);
    }
     */
}

