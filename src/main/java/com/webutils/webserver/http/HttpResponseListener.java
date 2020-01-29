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
    private String _uriOrStatus;
    private int _status;
    private String _versionOrReason;
    private List<HttpField> _fields = new ArrayList<>();
    private List<HttpField> _trailers = new ArrayList<>();
    private String[] _hdr;
    private String[] _val;
    private int _headers;
    private boolean _early;
    private boolean _headerCompleted;
    private boolean _messageCompleted;
    private final List<HttpComplianceSection> _complianceViolation = new ArrayList<>();

    private HttpResponseCallback httpResponseCb;

    public HttpResponseListener(final HttpResponseCallback callback) {
        httpResponseCb = callback;
    }

    @Override
    public boolean content(ByteBuffer ref) {
        if (_content == null)
            _content = "";
        String c = BufferUtil.toString(ref, StandardCharsets.UTF_8);
        _content = _content + c;
        ref.position(ref.limit());

        LOG.info("content(resp) _content: " + _content);

        return false;
    }

    @Override
    public void parsedHeader(HttpField field) {
        _fields.add(field);
        _hdr[++_headers] = field.getName();
        _val[_headers] = field.getValue();

        LOG.info("parseHeader(resp) _headers: " + _headers + " _hdr: " + field.getName() +
                " _val: " + field.getValue());

        if (field instanceof HostPortHttpField) {
            HostPortHttpField hpfield = (HostPortHttpField) field;
            _host = hpfield.getHost();
            _port = hpfield.getPort();

            LOG.info("parseHeader(resp) _host: " + _host + " _port: " + _port);
        }
    }

    @Override
    public boolean headerComplete() {
        LOG.info("headerComplete(resp)");
        _content = null;
        _headerCompleted = true;
        return false;
    }

    @Override
    public void parsedTrailer(HttpField field) {
        LOG.info("parsedTrailer(resp) " + field.getValue());
        _trailers.add(field);
    }

    @Override
    public boolean contentComplete() {
        LOG.info("contentComplete(resp)");

        return false;
    }

    @Override
    public boolean messageComplete() {
        LOG.info("messageComplete(resp)");

        _messageCompleted = true;

        httpResponseCb.httpResponse(_status, _headerCompleted, _messageCompleted);
        return true;
    }

    @Override
    public void badMessage(BadMessageException failure) {
        String reason = failure.getReason();
        _bad = reason == null ? String.valueOf(failure.getCode()) : reason;

        LOG.info("badMessage(resp) reason: " + reason);
    }

    @Override
    public boolean startResponse(HttpVersion version, int status, String reason) {
        LOG.info("StartResponse(resp) version: " + version + " status: " + status +
                " reason: " + reason);
        _fields.clear();
        _trailers.clear();
        _methodOrVersion = version.asString();
        _uriOrStatus = Integer.toString(status);
        _status = status;
        _versionOrReason = reason;
        _headers = -1;
        _hdr = new String[10];
        _val = new String[10];
        _messageCompleted = false;
        _headerCompleted = false;
        return false;
    }

    @Override
    public void earlyEOF() {
        LOG.info("earlyEOF(resp)");
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

