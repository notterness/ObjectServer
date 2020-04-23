package com.webutils.webserver.http.parser;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpRequestInfo;
import org.eclipse.jetty.http.*;
import org.eclipse.jetty.util.BufferUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpParserListener implements HttpParser.RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpParserListener.class);

    /*
     ** TODO: These local variables will be removed from this class once the code is further along.
     **   They are temporary placeholders until the rest of the HTTP response path is worked
     **   through.
     */

    private String _versionOrReason;

    private String _content;
    private List<HttpField> _trailers = new ArrayList<>();
    //private final List<HttpComplianceSection> _complianceViolation = new ArrayList<>();

    private final HttpInfo httpRequestInfo;

    public HttpParserListener(final HttpInfo httpHeaderInfo) {
        this.httpRequestInfo = httpHeaderInfo;
    }

    @Override
    public boolean content(ByteBuffer ref) {
        if (_content == null)
            _content = "";
        String c = BufferUtil.toString(ref, StandardCharsets.UTF_8);
        _content = _content + c;
        ref.position(ref.limit());

        LOG.info("content() _content: " + _content);

        return false;
    }

    @Override
    public boolean startRequest(String method, String uri, HttpVersion version) {
        /*
        ** Parse the URI first to obtain any version information
         */
        httpRequestInfo.setHttpUri(uri);

        if (_versionOrReason != null) {
            LOG.info("StartRequest() method: " + method + " uri: " + uri +
                    " version: " + _versionOrReason);
        } else {
            LOG.info("StartRequest() method: " + method + " uri: " + uri +
                    " version: null");
        }

        httpRequestInfo.setHttpMethodAndVersion(method, _versionOrReason);
        return false;
    }

    @Override
    public void parsedHeader(HttpField field) {
        httpRequestInfo.addHeaderValue(field);
    }

    @Override
    public boolean headerComplete() {
        LOG.info("headerComplete()");
        httpRequestInfo.setHeaderComplete();
        return false;
    }

    @Override
    public void parsedTrailer(HttpField field) {
        LOG.info("parsedTrailer() " + field.toString());

        _trailers.add(field);
    }

    @Override
    public boolean contentComplete() {
        LOG.info("contentComplete()");

        httpRequestInfo.setContentComplete();
        return false;
    }

    @Override
    public boolean messageComplete() {
        LOG.info("messageComplete()");

        httpRequestInfo.setMessageComplete();
        return true;
    }

    @Override
    public void badMessage(final BadMessageException failure) {
        httpRequestInfo.httpHeaderError(failure);
    }

    @Override
    public void earlyEOF() {
        httpRequestInfo.earlyEndOfFile();
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

