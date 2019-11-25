package com.oracle.athena.webserver.connectionstate;

import com.google.common.io.BaseEncoding;
import com.oracle.pic.casper.common.exceptions.InvalidMd5Exception;
import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HostPortHttpField;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CasperHttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(CasperHttpInfo.class);

    /*
     ** The connection this HTTP information is associated with
     */
    private WebServerConnState connectionState;

    private boolean headerComplete;
    private boolean contentComplete;
    private boolean messageComplete;

    private boolean earlyEof;


    /*
     ** HTTP level
     **
     *** Expected: "HTTP/1.1"
     */
    private String httpVersion;

    /*
     ** Method is one of POST, PUT, DELETE, GET, HEAD, TRACE
     */
    private String httpMethodString;
    private HttpMethodEnum httpMethod;


    /*
     ** Response for this operation
     **
     ** See values in HttpStatus
     */
    private int responseCode;

    /*
    ** The following variables are set when httpHeaderError() is called to indicate there was a problem with the
    **   buffer passed into the HTTP Parser.
     */
    private int parseFailureCode;
    private String parseFailureReason;

    private boolean contentLengthReceived;


    /*
     **
     */
    private String httpHost;
    private int httpPort;

    private String contentType;

    private String UserAgent;

    private long contentLength;

    /*
     **
     */
    private String namespace;

    /*
     ** Name of the object being created within Casper
     */
    private String name;

    /*
     ** Compartment where the object is being created within Casper.
     **
     *** i.e. : "ocid1.tenancy.oc1..aaaaaaaaaodntvb6nij46dccx2dqn6a3xs563vhqm7ay5bkn4wbqvb2a3bya"
     */
    private String compartmentId;

    /*
     ** Id of the object being created within Casper
     **
     ** i.e. : "ocid1.bucket.dev.dev.aaaaaaaajitbjrzo4sy56einy6wlwv46etjg62hn5t7we4yqfwfbf77q7syq"
     */
    private String id;

    /*
     **
     ** i.e. : dev-1:ujuBuIUfkVu8M6pTiIc6hEy_-9OS2iG9aTK9xg0NbeX2jzleUkmCAEPGPhgVQGAC
     */
    private String opcRequestId;

    private String md5_Digest;

    /*
     ** Where the object will be placed within Casper:
     *
     ** "Standard" or "Archive"
     */
    private String storageTier;

    /*
     ** "NoPublicAccess"
     */
    private String publicAccessType;

    /*
     ** This is a unique identifier for the object within Casper
     **
     ** i.e. : "32eae9f1-abd1-4cdd-9685-428a3fe28f65"
     */
    private String etag;

    /*
     ** "Disabled"
     */
    private String objectLevelAuditMode;

    /*
     ** Setting is in the format:
     ** {
     **    "empty": true
     ** }
     */
    private String meterFlagSet;

    private String kmsKeyId;

    private String freeFormTags;

    private String definedTags;

    private int approximateCount;

    private int approximateSize;

    private boolean objectEventsEnabled;

    private boolean replicationEnabled;
    private boolean isReadOnly;

    private String replicationSources;

    private String createdBy;

    /*
     ** Format: "2019-10-22T15:44:28.239Z"
     */
    private String timeCreated;

    private String lastModified;

    /*
     ** Used to keep track of the Header fields in a generic manner
     */
    private List<HttpField> _fields = new ArrayList<>();
    private String[] _hdr;
    private String[] _val;
    private int _headers;

    /*
    ** This is used to determine the method as an enum from the method string
     */
    private Map<HttpMethodEnum, String> httpMethodMap;

    CasperHttpInfo(final WebServerConnState connState) {
        headerComplete = false;
        contentComplete = false;
        messageComplete = false;
        earlyEof = false;

        _fields.clear();
        _headers = -1;
        _hdr = new String[10];
        _val = new String[10];

        parseFailureCode = 0;
        parseFailureReason = null;
        contentLengthReceived = false;

        /*
         ** Need the ConnectionState to know who to inform when the different
         **   HTTP parsing phases complete.
         */
        connectionState = connState;

        httpMethod = HttpMethodEnum.INVALID_METHOD;

        /*
        ** Create a map of the HTTP methods to make the parsing easier
         */
        httpMethodMap = new HashMap<>(2);
        httpMethodMap.put(HttpMethodEnum.PUT_METHOD, "PUT");
        httpMethodMap.put(HttpMethodEnum.POST_METHOD, "POST");
    }


    /*
     ** Clear out all of the String fields and release the memory
     */
    void reset() {

        /*
         ** Clear the complete booleans so that the message is not assumed to be parsed.
         */
        headerComplete = false;
        contentComplete = false;
        messageComplete = false;
        earlyEof = false;

        /*
         **
         */
        _fields.clear();
        _headers = -1;
        _hdr = new String[10];
        _val = new String[10];

        /*
        ** Clear the failure reasons
         */
        parseFailureCode = 0;
        parseFailureReason = null;
        contentLengthReceived = false;

        /*
         ** Clear the strings from the various fields so the resources can be released.
         */
        httpVersion = null;

        httpMethodString = null;
        httpMethod = HttpMethodEnum.INVALID_METHOD;

        responseCode = HttpStatus.OK_200;

        httpHost = null;
        httpPort = 0;

        contentType = null;
        UserAgent = null;

        contentLength = 0;

        namespace = null;
        name = null;
        compartmentId = null;
        id = null;

        opcRequestId = null;
        md5_Digest = null;

        storageTier = "Standard";

        etag = null;
        objectLevelAuditMode = "Disabled";

        meterFlagSet = null;
        kmsKeyId = null;
        freeFormTags = null;
        definedTags = null;

        approximateCount = 0;
        approximateSize = 0;

        objectEventsEnabled = false;
        replicationEnabled = false;
        isReadOnly = false;
        replicationSources = null;

        createdBy = null;
        timeCreated = null;
        lastModified = null;
    }

    public void setHttpMethodAndVersion(String methodString, String httpParsedVersion) {
        httpMethodString = methodString;
        httpVersion = httpParsedVersion;

        /*
        ** Determine the method enum based upon the passed in method string
         */
        for (Map.Entry<HttpMethodEnum, String> entry: httpMethodMap.entrySet()) {
            int result = methodString.indexOf(entry.getValue());
            if (result != -1) {
                httpMethod = entry.getKey();
                break;
            }
        }
    }

    public HttpMethodEnum getMethod() {
        return httpMethod;
    }

    public void setHostAndPort(final String host, final int port) {
        httpHost = host;
    }

    /*
     ** This function will pull out the various information in the HTTP header fields and add it to
     ** the associated string within this object.
     ** TODO: Is it more efficient to add the values to a particular named field or to keep them in the
     **   _fields + _hdr + _val fields?
     */
    public void addHeaderValue(HttpField field) {
        _fields.add(field);
        _hdr[++_headers] = field.getName();
        _val[_headers] = field.getValue();

        LOG.info("addHeaderValue() _headers: " + _headers + " _hdr: " + field.getName() +
                " _val: " + field.getValue());

        if (field instanceof HostPortHttpField) {
            HostPortHttpField hpfield = (HostPortHttpField) field;
            httpHost = hpfield.getHost();
            httpPort = hpfield.getPort();

            LOG.info("addHeaderValue() _host: " + httpHost + " _port: " + httpPort);
            return;
        }

        int result = _hdr[_headers].indexOf("Content-Length");
        if (result != -1) {
            try {
                contentLengthReceived = true;
                contentLength = Long.parseLong(_val[_headers]);

                /*
                 ** TODO: Are there specific limits for the Content-Length that need to be validated
                 */
                if (contentLength < 0) {
                    contentLength = 0;
                    parseFailureCode = HttpStatus.RANGE_NOT_SATISFIABLE_416;
                    parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                    LOG.info("Invalid Content-Length [" + connectionState.getConnStateId() +  "] code: " +
                            parseFailureCode + " reason: " + parseFailureReason);

                    connectionState.setHttpParsingError();
                }
            } catch (NumberFormatException num_ex) {
                LOG.info("addHeaderValue() " + _val[_headers] + " " + num_ex.getMessage());
            }
            return;
        }

        result = _hdr[_headers].indexOf("Content-MD5");
        if (result != -1) {
            LOG.info("md5_Digest(start): " + _val[_headers]);

            try {
                byte[] bytes = BaseEncoding.base64().decode(_val[_headers]);
                if (bytes.length != 16) {
                    throw new InvalidMd5Exception("The value of the Content-MD5 header '" + _val[_headers] +
                            "' was not the correct length after base-64 decoding");
                } else {
                    LOG.info("md5_Digest: " + _val[_headers]);
                }
                md5_Digest = _val[_headers];
            } catch (IllegalArgumentException iaex) {
                throw new InvalidMd5Exception("The value of the Content-MD5 header '" + _val[_headers] +
                        "' was not the correct length after base-64 decoding");
            }
        }
    }

    /*
     ** When the headers have been completely read in, that will be the time to insure it is valid
     **   and the field values make sense.
     */
    public void setHeaderComplete() {
        /*
        ** Verify that the "Content-Length" header has been received. It is an error if it has not
         */
        if (!contentLengthReceived) {
            parseFailureCode = HttpStatus.NO_CONTENT_204;
            parseFailureReason = HttpStatus.getMessage(parseFailureCode);

            LOG.info("No Content-Length [" + connectionState.getConnStateId() +  "] code: " +
                    parseFailureCode + " reason: " + parseFailureReason);

            connectionState.setHttpParsingError();
        }

        headerComplete = true;

        connectionState.httpHeaderParseComplete(contentLength);
    }

    public boolean getHeaderComplete() {
        return headerComplete;
    }

    public void setContentComplete() {
        contentComplete = true;
    }

    public void setMessageComplete() {
        messageComplete = true;
    }

    /*
    ** This is called when the Jetty HTTP parser calls badMessage() to set the parsing error
     */
    public void httpHeaderError(final BadMessageException failure) {
        String reason = failure.getReason();

        parseFailureCode = failure.getCode();
        parseFailureReason = (reason == null) ? String.valueOf(failure.getCode()) : reason;
        LOG.info("badMessage() [" + connectionState.getConnStateId() + "] code: " +
                parseFailureCode + " reason: " + parseFailureReason);

        connectionState.setHttpParsingError();
    }

    public int getParseFailureCode() {
        return parseFailureCode;
    }


    /*
     ** Something terminated the HTTP transfer early.
     **
     ** TODO: Handle the error and cleanup the connection.
     */
    public void setEarlyEof() {
        earlyEof = true;
    }
}
