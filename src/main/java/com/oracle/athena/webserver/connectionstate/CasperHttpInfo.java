package com.oracle.athena.webserver.connectionstate;

import org.eclipse.jetty.http.HostPortHttpField;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;

import java.util.ArrayList;
import java.util.List;

public class CasperHttpInfo {

    /*
     ** The connection this HTTP information is associated with
     */
    private ConnectionState connectionState;

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
    private String httpMethod;

    /*
     ** Response for this operation
     **
     ** See values in HttpStatus
     */
    private int responseCode;

    /*
     **
     */
    private String httpHost;
    private int httpPort;

    private String ContentType;

    private String UserAgent;

    private int ContentLength;

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

    private String opcContentMd5;

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

    private int approiximateSize;

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


    CasperHttpInfo(final ConnectionState connState) {
        headerComplete = false;
        contentComplete = false;
        messageComplete = false;
        earlyEof = false;

        _fields.clear();
        _headers = -1;
        _hdr = new String[10];
        _val = new String[10];

        /*
         ** Need the ConnectionState to know who to inform when the different
         **   HTTP parsing phases complete.
         */
        connectionState = connState;
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
         ** Clear the strings from the various fields so the resources can be released.
         */
        httpVersion = null;

        httpMethod = null;

        responseCode = HttpStatus.OK_200;

        httpHost = null;
        httpPort = 0;

        ContentType = null;
        UserAgent = null;

        ContentLength = 0;

        namespace = null;
        name = null;
        compartmentId = null;
        id = null;

        opcRequestId = null;
        opcContentMd5 = null;

        storageTier = "Standard";

        etag = null;
        objectLevelAuditMode = "Disabled";

        meterFlagSet = null;
        kmsKeyId = null;
        freeFormTags = null;
        definedTags = null;

        approximateCount = 0;
        approiximateSize = 0;

        objectEventsEnabled = false;
        replicationEnabled = false;
        isReadOnly = false;
        replicationSources = null;

        createdBy = null;
        timeCreated = null;
        lastModified = null;
    }

    public void setHttpMethodAndVersion(String method, String httpParsedVersion) {
        httpMethod = method;

        httpVersion = httpParsedVersion;
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

        System.out.println("addHeaderValue() _headers: " + _headers + " _hdr: " + field.getName() +
                " _val: " + field.getValue());

        if (field instanceof HostPortHttpField) {
            HostPortHttpField hpfield = (HostPortHttpField) field;
            httpHost = hpfield.getHost();
            httpPort = hpfield.getPort();

            System.out.println("addHeaderValue() _host: " + httpHost + " _port: " + httpPort);
        }
    }

    /*
     ** When the header has been completely read in, that will be the time to insure it is valid
     **   and the field values make sense.
     */
    public void setHeaderComplete() {
        headerComplete = true;

        connectionState.httpHeaderParseComplete();
    }

    public void setContentComplete() {
        contentComplete = true;
    }

    public void setMessageComplete() {
        messageComplete = true;
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
