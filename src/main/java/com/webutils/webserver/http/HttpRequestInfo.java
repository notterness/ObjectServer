package com.webutils.webserver.http;

import com.google.common.io.BaseEncoding;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HostPortHttpField;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HttpRequestInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestInfo.class);

    /**
     * The following 3 headers are secret headers that should only be used internally by cross-region replication
     * workers.
     */
    private static final String MD5_OVERRIDE_HEADER = "md5-override";

    private static final String CONTENT_LENGTH = "Content-Length";
    private static final String CONTENT_MD5 = "Content-MD5";

    private static final String CONTENT_SHA256 = "x-content-sha256";

    private static final String OBJECT_NAME = "/o/";
    private static final String NAMESPACE_NAME = "/n/";
    private static final String BUCKET_NAME = "/b/";
    private static final String TEST_TYPE = "/t/";


    /*
     ** The connection this HTTP information is associated with
     */
    private final RequestContext requestContext;

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

    private String successResponseHeaders;
    private String successResponseContent;

    private boolean contentLengthReceived;


    /*
     **
     */
    private String httpHost;
    private int httpPort;

    private int contentLength;

     /*
     ** This defines the redundancy and placement for the Objects within a bucket
     */
    private StorageTierEnum storageTier;

    /*
    ** This is the unique identifier to access the Object record within the ObjectStorageDd after it has been created.
    **
    ** NOTE: This is only filled in for operations that operate on Objects
     */
    private int objectId;

    /*
    ** md5override comes from the "md5-override" header (MD5_OVERRIDE_HEADER)
     */
    private String md5Override;

    /*
     ** This comes from the "Content-MD5" header (CONTENT_MD5). If the validation of
     **   the passed in Content-MD5 header fails, expectedMd5 will be set to null. There
     **   is the case that there is no "Content-MD5" header in which case, md5parsed will
     **   be set to false;
     */
    private String expectedMD5;
    private boolean md5parsed;
    private String contentMD5;

    /*
     ** This comes from the "x-content-sha256" header (CONTENT_SHA256). If the validation of
     **   the passed in Content-MD5 header fails, expectedMd5 will be set to null. There
     **   is the case that there is no "Content-MD5" header in which case, md5parsed will
     **   be set to false;
     */
    private String expectedSha256;
    private boolean sha256Parsed;


    /*
     ** Used to keep track of the Header fields in a generic manner
     */
    private final Map<String, List<String>> headers;

    private final String[] uriFields = {OBJECT_NAME, BUCKET_NAME, NAMESPACE_NAME, TEST_TYPE};


    /*
    ** This is used to determine the method as an enum from the method string
     */
    private final Map<HttpMethodEnum, String> httpMethodMap;

    /*
    ** This map is used to hold the Object Name, Bucket Name and Tenancy Name for the created objected.
    ** For Storage Servers, there is also a Test Type that is used to force certain behaviors in the
    **   Storage Server's responses (i.e. disconnect the connection).
     */
    private final Map<String, String> putObjectInfoMap;


    public HttpRequestInfo(final RequestContext requestContext) {
        headerComplete = false;
        contentComplete = false;
        messageComplete = false;
        earlyEof = false;
        parseFailureCode = 0;
        parseFailureReason = null;
        contentLengthReceived = false;

        // provide case-insensitive key management for the headers map using get(), containsKey(), and put()
        headers = new HashMap<>() {
            @Override
            public List<String> get(Object key) {
                return super.get(key.toString().toLowerCase());
            }
            @Override
            public boolean containsKey(Object key) {
                return super.containsKey(key.toString().toLowerCase());
            }
            @Override
            public List<String> put(String key, List<String> value) {
                return super.put(key.toLowerCase(), value);
            }
        };

        /*
         ** Need the ConnectionState to know who to inform when the different
         **   HTTP parsing phases complete.
         */
        this.requestContext = requestContext;

        httpMethod = HttpMethodEnum.INVALID_METHOD;
        expectedMD5 = null;
        contentMD5 = null;
        md5parsed = false;

        /*
        ** These are used to return specific information when requests are successful.
         */
        successResponseContent = null;
        successResponseHeaders = null;

        /*
        ** Create a map of the HTTP methods to make the parsing easier
         */
        httpMethodMap = new HashMap<>(2);
        httpMethodMap.put(HttpMethodEnum.PUT_METHOD, "PUT");
        httpMethodMap.put(HttpMethodEnum.POST_METHOD, "POST");

        putObjectInfoMap = new HashMap<>(3);

        /*
        ** Set the objectId to -1 to indicate that it is not valid.
        ** Set the storageTier to the default value of STANDARD_TIER. The value for the storageTier comes from the
        **   Bucket that the Object is being stored in.
         */
        objectId = -1;
        storageTier = StorageTierEnum.STANDARD_TIER;
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

        httpMethod = HttpMethodEnum.INVALID_METHOD;

        responseCode = HttpStatus.OK_200;

        httpHost = null;
        httpPort = 0;

        contentLength = 0;

        expectedMD5 = null;
        contentMD5 = null;
        md5parsed = false;

        storageTier = StorageTierEnum.STANDARD_TIER;
        objectId = -1;

        successResponseContent = null;
        successResponseHeaders = null;

        /*
        ** Clear out the object information map
         */
        for (String uriField : uriFields) {
            putObjectInfoMap.remove(uriField);
        }
    }

    /*
    ** Determine the HTTP response handler based upon the URI and some header fields.
     */
    public void setHttpMethodAndVersion(String methodString, String httpParsedVersion) {
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

    public void setObjectId(final int id) {
        objectId = id;
    }

    public int getObjectId() { return objectId; }


    /*
     ** The uri for the request. This is where the object name, tenancy and bucket name come from
     */
    public void setHttpUri(final String uri) {

        /*
         ** Find the information about this object from the HTTP URI
         */
        for (String uriField : uriFields) {
            String tmp = null;
            int startingIndex = uri.indexOf(uriField);
            if (startingIndex != -1) {
                startingIndex += uriField.length();
                int endingIndex = uri.indexOf(' ', startingIndex);
                if (endingIndex == -1) {
                    if ((endingIndex = uri.indexOf('/', startingIndex)) == -1) {
                        endingIndex = uri.length();
                    }
                }

                if (endingIndex != -1) {
                    try {
                        tmp = uri.substring(startingIndex, endingIndex);
                        LOG.info("setHttpUri() [" + requestContext.getRequestId() + "] name: " + uriField + " name: " + tmp);
                    } catch (IndexOutOfBoundsException ex) {
                        LOG.warn("setHttpUri() [" + requestContext.getRequestId() + "] name:" + uriField + " startingIndex: " + startingIndex + " endingIndex: " + endingIndex);
                    }
                }
            } else {
                LOG.warn("setHttpUri() [" + requestContext.getRequestId() + "] name: " + uriField + " is null");
            }

            putObjectInfoMap.put(uriField, tmp);
        }
    }

    /*
     ** This function will pull out the various information in the HTTP header fields and add it to
     ** the associated string within this object.
     ** TODO: Is it more efficient to add the values to a particular named field or to keep them in the
     **   _fields + _hdr + _val fields?
     */
    public void addHeaderValue(HttpField field) {
        final String fieldName = field.getName().toLowerCase();
        if (!headers.containsKey(fieldName)) {
            headers.put(fieldName, new ArrayList<>());
        }
        headers.get(fieldName).add(field.getValue());

        LOG.info("addHeaderValue() header.name" +  fieldName + " value: " + field.getValue());

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
        int result = fieldName.indexOf(CONTENT_LENGTH.toLowerCase());
        if (result != -1) {
            try {
                contentLengthReceived = true;
                contentLength = Integer.parseInt(field.getValue());

                /*
                 ** TODO: Are there specific limits for the Content-Length that need to be validated
                 */
                if (contentLength < 0) {
                    contentLength = 0;
                    parseFailureCode = HttpStatus.RANGE_NOT_SATISFIABLE_416;
                    parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                    LOG.info("Invalid Content-Length [" + requestContext.getRequestId() +  "] code: " +
                            parseFailureCode + " reason: " + parseFailureReason);

                    requestContext.setHttpParsingError();
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

            LOG.warn("No Content-Length [" + requestContext.getRequestId() +  "] code: " +
                    parseFailureCode + " reason: " + parseFailureReason);

            requestContext.setHttpParsingError();
        }

        /*
        ** Verify that there was an Object Name, Bucket Name and Tenant Name passed in
         */
        if (httpMethod == HttpMethodEnum.PUT_METHOD) {
            if ((getObject() == null) || (getBucket() == null) || (getNamespace() == null)) {
                parseFailureCode = HttpStatus.BAD_REQUEST_400;
                parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                LOG.warn("PUT Missing Critical Object Info [" + requestContext.getRequestId() + "] code: " +
                        parseFailureCode + " reason: " + parseFailureReason);

                requestContext.setHttpParsingError();
            }
        } else if (httpMethod == HttpMethodEnum.POST_METHOD) {
            if ((getBucket().compareTo("") != 0) || (getNamespace() == null)) {

                LOG.error("bucket: " + getBucket() + " compare: " + getBucket().compareTo(""));
                LOG.error("namespace: " + getNamespace());

                parseFailureCode = HttpStatus.BAD_REQUEST_400;
                parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                LOG.warn("POST Missing Critical Object Info [" + requestContext.getRequestId() + "] code: " +
                        parseFailureCode + " reason: " + parseFailureReason);

                requestContext.setHttpParsingError();
            }
        }

        headerComplete = true;

        requestContext.httpHeaderParseComplete(contentLength);
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
        LOG.info("badMessage() [" + requestContext.getRequestId() + "] code: " +
                parseFailureCode + " reason: " + parseFailureReason);

        requestContext.setHttpParsingError();
    }

    public void setParseFailureCode(final int errorCode) {
        parseFailureCode = errorCode;
        parseFailureReason = HttpStatus.getMessage(parseFailureCode);
        requestContext.setHttpParsingError();
    }

    public void setParseFailureCode(final int errorCode, final String failureMessage) {
        parseFailureCode = errorCode;
        parseFailureReason = failureMessage;
        requestContext.setHttpParsingError();
    }

    public int getParseFailureCode() {
        return parseFailureCode;
    }

    public String getParseFailureReason() { return parseFailureReason; }


    /*
    ** These are the setters and getters for response headers and response content
     */
    public void setResponseHeaders(final String responseHeaders) {
        successResponseHeaders = responseHeaders;
    }

    public void setResponseContent(final String responseContent) {
        successResponseContent = responseContent;
    }

    public String getResponseHeaders() {
        if (successResponseHeaders == null) {
            return "";
        }

        return successResponseHeaders;
    }

    public String getResponseContent() {
        if (successResponseContent == null) {
            return "";
        }

        return successResponseContent;
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
    ** This will parse out various pieces of information needed for the V2 PUT and POST commands into easily
    **   accessible String member variables. This is called when the headers have all been parsed by
    **   the Jetty parser and prior to moving onto reading in the content data.
     */
    public void parseHeaders() {
        expectedMD5 = getContentMD5Header();
        expectedSha256 = getContentSha256Header();
        md5Override = getHeaderString(MD5_OVERRIDE_HEADER);
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
        } catch (IllegalArgumentException ia_ex) {
            LOG.warn("The value of the Content-MD5 header '" + md5value +
                    "' was not the correct length after base-64 decoding");
            return null;
        }

        return md5value;
    }

    /*
     ** This extracts the expected MD5 checksum from the headers if it exists and then it validates that
     **   it is the correct length.
     ** Assuming it is found and the correct length it is then returned.
     */
    private String getContentSha256Header() {
        String sha256Value = getHeaderString(CONTENT_SHA256);
        if (sha256Value == null || sha256Value.isEmpty()) {
            sha256Parsed = false;
            return null;
        }

        sha256Parsed = true;
        try {
            byte[] bytes = BaseEncoding.base64().decode(sha256Value);
            if (bytes.length != 32) {
                LOG.warn("The value of the x-content-sha256 header '" + sha256Value +
                        "' was not the correct length after base-64 decoding");
                return null;
            } else {
                //LOG.info("expectedSha256: " + sha256Value);
            }
        } catch (IllegalArgumentException ia_ex) {
            LOG.warn("The value of the Content-MD5 header '" + sha256Value +
                    "' was not the correct length after base-64 decoding");
            return null;
        }

        return sha256Value;
    }

    /*
     ** Return "tenancy" from the PathParam
     */
    public String getTenancy() {
        return null;
    }

    /*
     ** Return the "tenancy" (NAMESPACE_NAME) that was parsed from the HTTP uri
     */
    public String getNamespace() {
        return putObjectInfoMap.get(NAMESPACE_NAME);
    }

    /*
     ** Return the "bucket" (BUCKET_NAME) that was parsed from the HTTP uri
     */
    public String getBucket() {
        return putObjectInfoMap.get(BUCKET_NAME);
    }

    /*
     ** Return the "object" (OBJECT_NAME) that was parsed from the HTTP uri
     */
    public String getObject() {
        return putObjectInfoMap.get(OBJECT_NAME);
    }

    /*
    ** Return the "opc-client-request-id". This is an optional field in the PUT Object request.
     */
    public String getOpcClientRequestId() { return null; }

    /*
    **
     */
    public int getContentLength() {
        if (contentLengthReceived) {
            return contentLength;
        }

        return -1;
    }

    /*
    **
     */
    public String getContentMd5() {
        return contentMD5;
    }


    /*
    ** Return the "Test Type" (TEST_TYPE) that was parsed from the HTTP uri (This is prefixed with the "/t/").
    **
    ** Currently, TestType is only used by Storage Servers
    **
    ** The current valid TestTypes are:
    **    - DisconnectAfterHeader
     */
    public String getTestType() { return putObjectInfoMap.get(TEST_TYPE); }

    /**
     * Performs an integrity check on the body of an HTTP request if the Content-MD5 header is available.
     *
     * If Content-MD5 is not present, this function does nothing, otherwise it computes the MD5 value for the body and
     * compares it to the value from the header.
     *
     * @param computedMd5 - The MD5 value computed from the content data read in.
     */
    public boolean checkContentMD5(String computedMd5) {
        /*
        ** Save away the computed MD5 for the content as it is passed back in the OK_200 response headers for the
        **   PUT Object request.
         */
        contentMD5 = computedMd5;

        if (!md5parsed || (md5Override != null))
        {
            LOG.warn("checkContentMd5() [" + requestContext.getRequestId() + "] md5parsed: " + md5parsed +
                    " md5Override: " + md5Override);
            return true;
        }

        if (expectedMD5 != null) {
            if (!expectedMD5.equals(computedMd5)) {
                LOG.warn("Content-MD5 [" + requestContext.getRequestId() +  "] did not match computed. expected: " +
                        expectedMD5 + " computed: " + computedMd5);

                parseFailureCode = HttpStatus.UNPROCESSABLE_ENTITY_422;
                parseFailureReason = HttpStatus.getMessage(parseFailureCode);
                requestContext.setHttpParsingError();
                return false;
            }
        } else {
            LOG.warn("Content-MD5 [" + requestContext.getRequestId() +  "] passed in was invalid. computed: " +
                    computedMd5);
            parseFailureCode = HttpStatus.BAD_REQUEST_400;
            parseFailureReason = HttpStatus.getMessage(parseFailureCode);
            requestContext.setHttpParsingError();
            return false;
        }

        LOG.warn("checkContentMd5() [" + requestContext.getRequestId() +  "] passed");
        return true;
    }

    /**
     * Performs an integrity check on the body of an HTTP request if the Content-MD5 header is available.
     *
     * If Content-MD5 is not present, this function does nothing, otherwise it computes the MD5 value for the body and
     * compares it to the value from the header.
     *
     * @param computedSha256 - The MD5 value computed from the content data read in.
     */
    public boolean checkContentSha256(final String computedSha256) {
        if (!sha256Parsed)
        {
            LOG.warn("checkContentSha256() [" + requestContext.getRequestId() + "] sha256Parsed: " + sha256Parsed);
            return true;
        }

        if (expectedSha256 != null) {
            if (!expectedSha256.equals(computedSha256)) {
                LOG.warn("x-content-sha256 [" + requestContext.getRequestId() +  "] did not match computed. expected: " +
                        expectedSha256 + " computed: " + computedSha256);

                parseFailureCode = HttpStatus.UNPROCESSABLE_ENTITY_422;
                parseFailureReason = HttpStatus.getMessage(parseFailureCode);
                requestContext.setHttpParsingError();
                return false;
            }
        } else {
            LOG.warn("x-content-sha256 [" + requestContext.getRequestId() +  "] passed in was invalid. computed: " +
                    computedSha256);
            parseFailureCode = HttpStatus.BAD_REQUEST_400;
            parseFailureReason = HttpStatus.getMessage(parseFailureCode);
            requestContext.setHttpParsingError();
            return false;
        }

        LOG.warn("checkContentSha256() [" + requestContext.getRequestId() +  "] passed");
        return true;
    }


    /*
    ** This finds all the occurrences of a passed in String in the headers key fields and adds those to the return
    **   string.
     */
    private String getHeaderString(String name) {
        List<String> values = headers.get(name);
        if (values == null) {
            return null;
        } else if (values.isEmpty()) {
            return "";
        } else {
            Iterator<String> valuesIterator = values.iterator();
            StringBuilder buffer = new StringBuilder(valuesIterator.next());

            while(valuesIterator.hasNext()) {
                buffer.append(',').append(valuesIterator.next());
            }

            return buffer.toString();
        }
    }

}
