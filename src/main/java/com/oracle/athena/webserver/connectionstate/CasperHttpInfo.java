package com.oracle.athena.webserver.connectionstate;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.common.exceptions.ContentMD5UnmatchedException;
import org.apache.commons.codec.binary.Hex;
import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HostPortHttpField;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;

import java.util.*;

import org.glassfish.jersey.internal.util.collection.StringKeyIgnoreCaseMultivaluedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;

public class CasperHttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(CasperHttpInfo.class);

    /**
     * The following 3 headers are secret headers that should only be used internally by cross-region replication
     * workers.
     */
    private static final String ETAG_OVERRIDE_HEADER = "etag-override";
    private static final String MD5_OVERRIDE_HEADER = "md5-override";
    private static final String PART_COUNT_OVERRIDE_HEADER = "partcount-override";
    private static final String POLICY_ROUND_HEADER = "policy-round";

    private static final String CONTENT_LENGTH = "Content-Length";
    private static final String CONTENT_MD5 = "Content-MD5";

    private static final String X_VCN_ID = "x-vcn-id";
    private static final String VCN_ID_CASPER_DEBUG_HEADER = "x-vcn-id-casper";



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
    ** md5override comes from the "md5-override" header (MD5_OVERRIDE_HEADER)
     */
    private String md5Override;

    /*
    ** etagOverride comes from the "etag-override" header (ETAG_OVERRIDE_HEADER)
     */
    private String etagOverride;

    /*
    ** partCountOverrideHeader comes from the "partcount-override" header (PART_COUNT_OVERRIDE_HEADER)
    ** partCountOverride is the parsed out Integer value fro the partCountOverrideHeader String.
     */
    private String partCountOverrideHeader;
    private Integer partCountOverride;

    /*
    ** etagRound comes from the "" header (POLICY_ROUND_HEADER)
     */
    private String etagRound;

    /*
     ** This comes from the "Content-MD5" header (CONTENT_MD5). If the validation of
     **   the passed in Content-MD5 header fails, expectedMd5 will be set to null. There
     **   is the case that there is no "Content-MD5" header in which case, md5parsed will
     **   be set to false;
     */
    private String expectedMD5;
    private boolean md5parsed;

    /*
    ** This comes from the "x-vcn-id" header (X_VCN_ID)
     */
    private String vcnId;

    /*
    ** This comes from the "x-vcn-id-casper" header (VCN_ID_CASPER_DEBUG_HEADER)
     */
    private String vcnDebugId;

    /*
     ** Used to keep track of the Header fields in a generic manner
     */
    private final MultivaluedMap<String, String> headers;


    /*
    ** This is used to determine the method as an enum from the method string
     */
    private Map<HttpMethodEnum, String> httpMethodMap;

    CasperHttpInfo(final WebServerConnState connState) {
        headerComplete = false;
        contentComplete = false;
        messageComplete = false;
        earlyEof = false;

        headers = new StringKeyIgnoreCaseMultivaluedMap();

        parseFailureCode = 0;
        parseFailureReason = null;
        contentLengthReceived = false;

        /*
         ** Need the ConnectionState to know who to inform when the different
         **   HTTP parsing phases complete.
         */
        connectionState = connState;

        httpMethod = HttpMethodEnum.INVALID_METHOD;
        expectedMD5 = null;
        md5parsed = false;

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
        expectedMD5 = null;
        md5parsed = false;

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
        headers.add(field.getName(), field.getValue());

        LOG.info("addHeaderValue() header.name" +  field.getName() +
                " value: " + field.getValue());

        if (field instanceof HostPortHttpField) {
            HostPortHttpField hpfield = (HostPortHttpField) field;
            httpHost = hpfield.getHost();
            httpPort = hpfield.getPort();

            LOG.info("addHeaderValue() httpHost: " + httpHost + " httpPort: " + httpPort);
            return;
        }

        /*
        ** The CONTENT_LENGTH is parsed out early as it is used in the headers parsed callback to
        **   setup the next stage of the connection pipeline.
         */
        int result = field.getName().indexOf(CONTENT_LENGTH);
        if (result != -1) {
            try {
                contentLengthReceived = true;
                contentLength = Long.parseLong(field.getValue());

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
                LOG.info("addHeaderValue() " + field.getName() + " " + num_ex.getMessage());
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

    /*
    ** This will parse out various pieces of information needed for the V2 PUT command into easily
    **   accessible String member variables. This is called when the headers have all been parsed by
    **   the Jetty parser and prior to moving onto reading in the content data.
     */
    public void parseHeaders() {
        expectedMD5 = getContentMD5Header();
        md5Override = getHeaderString(MD5_OVERRIDE_HEADER);
        etagOverride = getHeaderString(ETAG_OVERRIDE_HEADER);
        partCountOverrideHeader = getHeaderString(PART_COUNT_OVERRIDE_HEADER);
        try {
            partCountOverride = partCountOverrideHeader == null ? null : Integer.parseInt(partCountOverrideHeader);
        } catch (NumberFormatException e) {
            throw new BadRequestException("Cannot parse partCountOverrideHeader" + partCountOverrideHeader, e);
        }
        etagRound = getHeaderString(POLICY_ROUND_HEADER);

        vcnId = vcnIDFromRequest();
        vcnDebugId = getHeaderString(VCN_ID_CASPER_DEBUG_HEADER);
    }

    /*
    ** This extracts the expected MD5 checksum from the headers if it exists and then it validates that
    **   it is the correct length.
    ** Assuming it is found and the correct length it is then returned.
     */
    private String getContentMD5Header() {
        String md5value = getHeaderString(CONTENT_MD5);
        if (md5value == null || md5value.isEmpty()) {
            md5parsed = false;
            return null;
        }

        md5parsed = true;
        try {
            byte[] bytes = BaseEncoding.base64().decode(md5value);
            if (bytes.length != 16) {
                LOG.warn("The value of the Content-MD5 header '" + md5value +
                        "' was not the correct length after base-64 decoding");
                return null;
            } else {
                //LOG.info("expectedMD5: " + md5value);
            }
        } catch (IllegalArgumentException iaex) {
            LOG.warn("The value of the Content-MD5 header '" + md5value +
                    "' was not the correct length after base-64 decoding");
            return null;
        }

        return md5value;
    }

    /*
    ** Return "namespace" from the PathParam
     */
    public String getNamespace() {
        return null;
    }

    /*
    ** Return the "bucket" from the PathParam
     */
    public String getBucket() {
        return null;
    }

    /*
    ** Return the "object" from the PathParam
     */
    public String getObject() {
        return null;
    }

    /**
     * Return the VCN ID, if any, in the request.
     */
    private String vcnIDFromRequest() {
        return getHeaderString(X_VCN_ID);
    }

    /**
     * Performs an integrity check on the body of an HTTP request if the Content-MD5 header is available.
     *
     * If Content-MD5 is not present, this function does nothing, otherwise it computes the MD5 value for the body and
     * compares it to the value from the header.
     *
     * @param computedMd5 - The MD5 value computed from the content data read in.
     */
    public boolean checkContentMD5(String computedMd5) {
        if ((md5parsed == false) || (md5Override != null))
        {
            LOG.warn("checkContentMd5() [" + connectionState.getConnStateId() + "] md5parsed: " + md5parsed +
                    " md5Override: " + md5Override);
            return true;
        }

        if (expectedMD5 != null) {
            if (!expectedMD5.equals(computedMd5)) {
                LOG.warn("Content-MD5 [" + connectionState.getConnStateId() +  "] did not match computed. expected: " +
                        expectedMD5 + " computed: " + computedMd5);

                parseFailureCode = HttpStatus.UNPROCESSABLE_ENTITY_422;
                parseFailureReason = HttpStatus.getMessage(parseFailureCode);
                connectionState.setHttpParsingError();
                return false;
            }
        } else {
            LOG.warn("Content-MD5 [" + connectionState.getConnStateId() +  "] passed in was invalid. computed: " +
                    computedMd5);
            parseFailureCode = HttpStatus.BAD_REQUEST_400;
            parseFailureReason = HttpStatus.getMessage(parseFailureCode);
            connectionState.setHttpParsingError();
            return false;
        }

        LOG.warn("checkContentMd5() [" + connectionState.getConnStateId() +  "] passed");
        return true;
    }

    private static String computeBase64MD5(byte[] bytes) {
        return BaseEncoding.base64().encode(Hashing.md5().newHasher().putBytes(bytes).hash().asBytes());
    }

    private static String computeHexMD5(byte[] bytes) {
        return Hex.encodeHexString(Hashing.md5().newHasher().putBytes(bytes).hash().asBytes());
    }

    /*
    ** This finds all the occurrences of a passed in String in the headers key fields and adds those to the return
    **   string.
     */
    private String getHeaderString(String name) {
        List<String> values = (List)this.headers.get(name);
        if (values == null) {
            return null;
        } else if (values.isEmpty()) {
            return "";
        } else {
            Iterator<String> valuesIterator = values.iterator();
            StringBuilder buffer = new StringBuilder((String)valuesIterator.next());

            while(valuesIterator.hasNext()) {
                buffer.append(',').append((String)valuesIterator.next());
            }

            return buffer.toString();
        }
    }

}
