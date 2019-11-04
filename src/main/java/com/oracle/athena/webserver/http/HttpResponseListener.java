package com.oracle.athena.webserver.http;

import org.eclipse.jetty.http.*;
import org.eclipse.jetty.util.BufferUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class HttpResponseListener implements HttpParser.ResponseHandler {

    private String _host;
    private int _port;
    private String _bad;
    private String _content;
    private String _methodOrVersion;
    private String _uriOrStatus;
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

    @Override
    public boolean content(ByteBuffer ref) {
        if (_content == null)
            _content = "";
        String c = BufferUtil.toString(ref, StandardCharsets.UTF_8);
        _content = _content + c;
        ref.position(ref.limit());

        System.out.println("content(resp) _content: " + _content);

        return false;
    }

    @Override
    public void parsedHeader(HttpField field) {
        _fields.add(field);
        _hdr[++_headers] = field.getName();
        _val[_headers] = field.getValue();

        System.out.println("parseHeader(resp) _headers: " + _headers + " _hdr: " + field.getName() +
                " _val: " + field.getValue());

        if (field instanceof HostPortHttpField) {
            HostPortHttpField hpfield = (HostPortHttpField) field;
            _host = hpfield.getHost();
            _port = hpfield.getPort();

            System.out.println("parseHeader(resp) _host: " + _host + " _port: " + _port);
        }
    }

    @Override
    public boolean headerComplete() {
        System.out.println("headerComplete(resp)");
        _content = null;
        _headerCompleted = true;
        return false;
    }

    @Override
    public void parsedTrailer(HttpField field) {
        System.out.println("parsedTrailer(resp) " + field.getValue());
        _trailers.add(field);
    }

    @Override
    public boolean contentComplete() {
        System.out.println("contentComplete(resp)");

        return false;
    }

    @Override
    public boolean messageComplete() {
        System.out.println("messageComplete(resp)");

        _messageCompleted = true;
        return true;
    }

    @Override
    public void badMessage(BadMessageException failure) {
        String reason = failure.getReason();
        _bad = reason == null ? String.valueOf(failure.getCode()) : reason;

        System.out.println("badMessage(resp) reason: " + reason);
    }

    @Override
    public boolean startResponse(HttpVersion version, int status, String reason) {
        System.out.println("StartResponse(resp) version: " + version + " status: " + status +
                " reason: " + reason);
        _fields.clear();
        _trailers.clear();
        _methodOrVersion = version.asString();
        _uriOrStatus = Integer.toString(status);
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
        System.out.println("earlyEOF(resp)");
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
        System.out.println("onComplianceViolation()" + reason);
        _complianceViolation.add(violation);
    }
     */
}

