package com.oracle.athena.webserver.http.parser;

import com.oracle.athena.webserver.http.CasperHttpInfo;
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

    private String _host;
    private int _port;
    private String _bad;
    private String _content;
    private String _methodOrVersion;
    private String _versionOrReason;
    private List<HttpField> _trailers = new ArrayList<>();
    private boolean _early;
    private boolean _headerCompleted;
    private boolean _messageCompleted;
    private final List<HttpComplianceSection> _complianceViolation = new ArrayList<>();

    private CasperHttpInfo casperHttpInfo;

    public HttpParserListener(final CasperHttpInfo httpHeaderInfo) {
        casperHttpInfo = httpHeaderInfo;
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
        _methodOrVersion = method;
        _versionOrReason = version == null ? null : version.asString();
        _messageCompleted = false;
        _headerCompleted = false;
        _early = false;

        if (_versionOrReason != null) {
            LOG.info("StartRequest() method: " + method + " uri: " + uri +
                    " version: " + _versionOrReason);

            casperHttpInfo.setHttpMethodAndVersion(method, _versionOrReason);
        } else {
            LOG.info("StartRequest() method: " + method + " uri: " + uri +
                    " version: null");
        }

        casperHttpInfo.setHttpUri(uri);

        return false;
    }

    @Override
    public void parsedHeader(HttpField field) {
        casperHttpInfo.addHeaderValue(field);
    }

    @Override
    public boolean headerComplete() {
        LOG.info("headerComplete()");
        _content = null;
        _headerCompleted = true;

        casperHttpInfo.setHeaderComplete();
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

        casperHttpInfo.setContentComplete();
        return false;
    }

    @Override
    public boolean messageComplete() {
        LOG.info("messageComplete()");

        _messageCompleted = true;

        casperHttpInfo.setMessageComplete();
        return true;
    }

    @Override
    public void badMessage(final BadMessageException failure) {
        casperHttpInfo.httpHeaderError(failure);
    }

    @Override
    public void earlyEOF() {

        _early = true;
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

