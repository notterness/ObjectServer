package com.webutils.webserver.http;

import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HostPortHttpField;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/*
** This is a base class to contain information that is either parsed from the HTTP Request or the HTTP Response
 */
abstract public class HttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HttpInfo.class);

    /**
     *
     */
    private static final String MD5_OVERRIDE_HEADER = "md5-override";

    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CONTENT_MD5 = "Content-MD5";

    public static final String CONTENT_SHA256 = "x-content-sha256";

    public static final String CLIENT_OPC_REQUEST_ID = "opc-client-request-id";
    public static final String OPC_REQUEST_ID = "opc-request-id";

    private static final String OBJECT_NAME = "/o";
    private static final String NAMESPACE_NAME = "/n";
    private static final String BUCKET_NAME = "/b";
    private static final String TEST_TYPE = "/T";
    private static final String LIST_TYPE = "/l";
    private static final String TENANCY_NAME_URI = "/t";
    private static final String USER_NAME_URI = "/u";
    private static final String HEALTH_CHECK = "/health";

    public static final String TENANCY_NAME = "tenancy-name";
    public static final String CUSTOMER_NAME = "customer-name";
    private static final String CONNECTION_KEY = "connection-key";

    public static final String USER_NAME = "user-name";
    public static final String USER_PASSWORD = "user-password";

    public static final String ACCESS_TOKEN = "access-token";


    /*
     ** The following are used by the Storage Server to determine where to write the chunk data
     */
    public static final String CHUNK_LBA = "chunk-lba";
    public static final String CHUNK_NUMBER = "object-chunk-number";
    public static final String CHUNK_LOCATION = "chunk-location";
    public static final String CHUNK_ID = "chunk-id";


    /*
    ** The following headers are used for the GET and PUT commands
    **   "if-match" - Contains the ETag for the object
    **    "if-none-match" - May only contain '*'. This requires that the "if-match" header is provided. For GET
    **       operations, this means that upload any that do not match the ETag. For PUT operations, it means to fail
    **       the upload if there already exists an object with the ETag.
     */
    public static final String IF_MATCH = "if-match";
    public static final String IF_NONE_MATCH = "if-none-match";

    public static final String VERSION_ID = "versionId";

    /*
    ** The following is used to determine what information should be returned by the ListObjects GET method.
    **   The default fields to return are:
    **     name, size, time-created and md5
     */
    private static final String FIELDS_LIST = "fields";

    /*
    ** The following are used for the Health Check method
     */
    public static final String DISABLE_SERVICE = "disable-service";
    public static final String ENABLE_SERVICE = "enable-service";

    /*
     ** The connection this HTTP information is associated with
     */
    protected int requestId;

    /*
     ** Method is one of POST, PUT, DELETE, GET, HEAD, TRACE
     */
    protected HttpMethodEnum httpMethod;

    /*
     ** Used to keep track of the Header fields in a generic manner
     */
    protected final Map<String, List<String>> headers;

    private final String[] uriFields = {TENANCY_NAME_URI, USER_NAME_URI, OBJECT_NAME, BUCKET_NAME, NAMESPACE_NAME,
            LIST_TYPE, TEST_TYPE, HEALTH_CHECK};

    protected boolean headerComplete;

    /*
     ** This is used to determine the method as an enum from the method string
     */
    private final Map<HttpMethodEnum, String> httpMethodMap;

    /*
     ** This map is used to hold the Object Name, Bucket Name and Tenancy Name for the created objected.
     ** For Storage Servers, there is also a Test Type that is used to force certain behaviors in the
     **   Storage Server's responses (i.e. disconnect the connection).
     */
    private final Map<String, String> objectUriInfoMap;

    /*
     ** The following variables are set when httpHeaderError() is called to indicate there was a problem with the
     **   buffer passed into the HTTP Parser. These are used to build the response to the request.
     */
    protected int parseFailureCode;
    protected String parseFailureReason;

    /*
    ** The following are metrics that need to be emitted.
    **
    ** TODO: Implement the output of metrics and the handling of multiple metrics for method handlers.
     */
    private int metricCode;
    private String metricMessage;

    /*
     ** The httpHost and httpPort can be used to validate the connection and to limit traffic
     */
    private String httpHost;
    private int httpPort;

    /*
    ** The following are used to determine how much data follows the headers.
     */
    protected boolean contentLengthReceived;
    protected int contentLength;

    public HttpInfo() {

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

        headerComplete = false;

        /*
         ** Create a map of the HTTP methods to make the parsing easier
         */
        httpMethodMap = new HashMap<>(4);
        httpMethodMap.put(HttpMethodEnum.PUT_METHOD, "PUT");
        httpMethodMap.put(HttpMethodEnum.POST_METHOD, "POST");
        httpMethodMap.put(HttpMethodEnum.GET_METHOD, "GET");
        httpMethodMap.put(HttpMethodEnum.DELETE_METHOD, "DELETE");

        objectUriInfoMap = new HashMap<>(3);

        /*
        **
         */
        httpMethod = HttpMethodEnum.INVALID_METHOD;

        /*
        ** General place to hold error response information
         */
        parseFailureCode = HttpStatus.OK_200;
        parseFailureReason = null;

        /*
         ** The httpHost and httpPort can be used to validate the connection and to limit traffic
         */
        httpHost = null;
        httpPort = 0;

        /*
        **
         */
        contentLengthReceived = false;
        contentLength = 0;

        /*
        ** The requestId is a unique Id used to track this client request
         */
        requestId = -1;
    }

    public void setRequestId(final int requestTrackingId) { requestId = requestTrackingId; }

    /*
     ** Clear out all of the String fields and release the memory
     */
    void reset() {

        httpMethod = HttpMethodEnum.INVALID_METHOD;

        headerComplete = false;

        /*
         ** Clear out the object information map
         */
        for (String uriField : uriFields) {
            objectUriInfoMap.remove(uriField);
        }

        /*
         ** General place to hold error response information
         */
        parseFailureCode = HttpStatus.OK_200;
        parseFailureReason = null;

        /*
        ** The httpHost and httpPort can be used to validate the connection and to limit traffic
         */
        httpHost = null;
        httpPort = 0;

        /*
         ** Default condition for the Http Header
         */
        contentLengthReceived = false;
        contentLength = 0;

        requestId = -1;
    }

    /*
     ** Determine the HTTP Request handler based upon the URI and some header fields.
     **
     **  This is where PUT, POST, GET, DELETE, etc is turned into a more useful enum.
     */
    public void setHttpMethodAndVersion(String methodString) {
        /*
         ** Determine the method enum based upon the passed in method string. There is currently a special case for
         **   the ListObject method which uses the GET method, but excludes a value in the "/o" field.
         */
        for (Map.Entry<HttpMethodEnum, String> entry: httpMethodMap.entrySet()) {
            int result = methodString.indexOf(entry.getValue());
            if (result != -1) {
                httpMethod = entry.getKey();
                break;
            }
        }

        LOG.info("HTTP request method: " + httpMethod.toString());
    }

    public HttpMethodEnum getMethod() {
        return httpMethod;
    }

    /*
     ** The uri for the request. This is where the object name, tenancy and bucket name come from when they are part of the
     **   the request.
     **
     ** The URI fields are:
     **   /n
     **   /b
     **   /o
     **
     ** The general format is something like:
     **   /n/namespace/b/bucket/o/object HTTP/1.1
     **
     ** But, there is the odd cse for ListObjects where the format is (missing the trailing '/' and the object name):
     **   /n/namespace/b/bucket/o HTTP/1.1
     */
    public void setHttpUri(final String uri) {

        /*
         ** Find the information about this object from the HTTP URI
         */
        int lastIndex = uri.length();

        for (String uriField : uriFields) {
            String tmp = null;
            int startingIndex = uri.indexOf(uriField);
            if (startingIndex != -1) {
                /*
                ** The extracted URI field is useful for determining if this is a Health Check request
                **   and potentially if there are more URI fields that do not pass information, but the code
                **   needs to know that the field was passed in.
                 */
                String extractedUriField = uri.substring(startingIndex, startingIndex + uriField.length());
                startingIndex += uriField.length();

                if (startingIndex == lastIndex) {
                    /*
                    ** Nothing to do with this field as it is at the end of the string unless it is either
                    **   "/health", "/t" or "/u"
                    **   in which case, add an entry with the value set to "true" to indicate this is a
                    **   Health Check, PUT Tenancy or PUT user command.
                     */
                    if (extractedUriField.equalsIgnoreCase(HEALTH_CHECK)) {
                        objectUriInfoMap.put(uriField, "true");
                    } else if (extractedUriField.equalsIgnoreCase(TENANCY_NAME_URI)) {
                        objectUriInfoMap.put(uriField, "true");
                    } else if (extractedUriField.equalsIgnoreCase(USER_NAME_URI)) {
                        objectUriInfoMap.put(uriField, "true");
                    }
                    continue;
                }

                /*
                ** Check if this uri field has the trailing "/". If so, move past it as it is not part of the field
                 */
                int endingIndex = uri.indexOf('/', startingIndex);
                if (endingIndex == startingIndex) {
                    startingIndex++;
                }

                /*
                ** Now start the extraction of the uri field value
                 */
                endingIndex = uri.indexOf(' ', startingIndex);
                if (endingIndex == -1) {
                    if ((endingIndex = uri.indexOf('/', startingIndex)) == -1) {
                        endingIndex = uri.length();
                    }
                }

                if (endingIndex != startingIndex) {
                    try {
                        tmp = uri.substring(startingIndex, endingIndex);
                        LOG.info("setHttpUri() [" + requestId + "] name: " + uriField + " name: " + tmp);
                    } catch (IndexOutOfBoundsException ex) {
                        LOG.warn("setHttpUri() [" + requestId + "] name:" + uriField + " startingIndex: " + startingIndex + " endingIndex: " + endingIndex);
                    }
                }
            } else {
                LOG.warn("setHttpUri() [" + requestId + "] name: " + uriField + " is null");
            }

            if (tmp != null) {
                objectUriInfoMap.put(uriField, tmp);
            }
        }
    }

    /*
     ** This function will pull out the various information in the HTTP header fields and add it to
     ** the associated string within this object.
     */
    public void addHeaderValue(HttpField field) {
        final String fieldName = field.getName().toLowerCase();
        if (!headers.containsKey(fieldName)) {
            headers.put(fieldName, new ArrayList<>());
        }
        headers.get(fieldName).add(field.getValue());

        LOG.info("addHeaderValue() header.name " +  fieldName + " value: " + field.getValue());

        if (field instanceof HostPortHttpField) {
            HostPortHttpField hpfield = (HostPortHttpField) field;
            httpHost = hpfield.getHost();
            httpPort = hpfield.getPort();

            LOG.info("addHeaderValue() httpHost: " + httpHost + " httpPort: " + httpPort);
        }
    }

    /*
     ** When the headers have been completely read in, that will be the time to insure it is valid
     **   and the field values make sense.
     */
    abstract public void setHeaderComplete();

    public void setContentComplete() {
    }

    public void setMessageComplete() {
    }

    /*
    ** There was some sort of termination of the transfer
     */
    public void earlyEndOfFile() {

    }

    /*
    ** This is used to determine when the parsing of the headers is complete and the parser needs to start processing
    **   the content data (assuming the "Content-Length" header is not 0 or missing).
     */
    public boolean getHeaderComplete() {
        return headerComplete;
    }


    /*
    ** The following methods are used to deal with errors that need to be passed back to the client
     */
    /*
     ** This is called when the Jetty HTTP parser calls badMessage() to set the parsing error
     */
    public void httpHeaderError(final BadMessageException failure) {
        String reason = (failure.getReason() == null) ? String.valueOf(failure.getCode()) : failure.getReason();

        String failureMessage = "{\r\n  \"code\": " + failure.getCode() +
                "\r\n  \"message\": \"" + reason + "\"" +
                "\r\n}";
        setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);

        LOG.info("badMessage() [" + requestId + "] code: " +
                parseFailureCode + " reason: " + parseFailureReason);
    }

    public void setParseFailureCode(final int errorCode) {
        parseFailureCode = errorCode;
        parseFailureReason = HttpStatus.getMessage(parseFailureCode);
    }

    public void setParseFailureCode(final int errorCode, final String failureMessage) {
        parseFailureCode = errorCode;
        parseFailureReason = failureMessage;
    }

    public int getParseFailureCode() {
        return parseFailureCode;
    }

    public String getParseFailureReason() { return parseFailureReason; }

    /*
    ** The following is used to handle metrics for this method handler.
    **
    ** TODO: Implement the actual code to send this metric to a local handler
     */
    public void emitMetric(final int code, final String message) {
        metricCode = code;
        metricMessage = message;
    }

    /*
    ** The following are used to pull specific fields from the parsed URI (PathParams)
    **    getTenancySetInUri() - "/t/" field in the URI
    **    getUserSetInUri() - "/u/" field in the URI
    **    getNamespace() - "/n/" field in the URI
    **    getBucket() - "/b/" field in the URI
    **    getObject() - "/o/" field in the URI
    **    isHealthCheck() - "/health" field in the URI
    **    getTestType() - "/T/" field in the URI - NOT FOR PRODUCTION
    **
     */
    /*
     ** Return if the "Tenancy" (TENANCY_NAME_URI) that was parsed from the HTTP uri. This is ths case when only "/t"
     **  is passed in the URI and there is not information associated with the "/t".
     */
    public boolean getTenancySetInUri() {
        String tenancy = objectUriInfoMap.get(TENANCY_NAME_URI);
        return ( (tenancy != null) && tenancy.equals("true") );
    }

    /*
     ** Return if the "Tenancy" (TENANCY_NAME_URI) that was parsed from the HTTP uri
     */
    public String getTenancyFromUri() {
        return objectUriInfoMap.get(TENANCY_NAME_URI);
    }

    /*
     ** Return if the "User" (USER_NAME_URI) that was parsed from the HTTP uri
     */
    public boolean getUserSetInUri() {
        String user = objectUriInfoMap.get(USER_NAME_URI);
        return ( (user != null) && user.equals("true") );
    }

    /*
     ** Return the "namespace" (NAMESPACE_NAME) that was parsed from the HTTP uri
     */
    public String getNamespace() {
        return objectUriInfoMap.get(NAMESPACE_NAME);
    }

    /*
     ** Return the "bucket" (BUCKET_NAME) that was parsed from the HTTP uri
     */
    public String getBucket() {
        return objectUriInfoMap.get(BUCKET_NAME);
    }

    /*
     ** Return the "object" (OBJECT_NAME) that was parsed from the HTTP uri
     */
    public String getObject() {
        return objectUriInfoMap.get(OBJECT_NAME);
    }

    /*
    ** Return the "list type" (LIST_TYPE, "/l") that was parsed from the HTTP uri. This is only used by the
    **   ChunkMgr service to differentiate between the ListChunks and ListServers GET methods.
    **     ListChunks wil have "/l/chunks"
    **     ListServers will have "/l/servers"
     */
    public String getListType() {
        /*
        ** Validate that the string is one of the expected ones
         */
        String listType = objectUriInfoMap.get(LIST_TYPE);

        if ( (listType != null) && (listType.equals("chunks") || listType.equals("servers")) ) {
            return listType;
        }

        return null;
    }

    /*
     ** Return the "Test Type" (TEST_TYPE) that was parsed from the HTTP uri (This is prefixed with the "/t/").
     **
     ** Currently, TestType is only used by Storage Servers
     **
     ** The current valid TestTypes are:
     **    - DisconnectAfterHeader
     */
    public String getTestType() { return objectUriInfoMap.get(TEST_TYPE); }

    /*
    ** Return if this is a Health Check request
     */
    public boolean isHealthCheck() {
        String healthCheckStr = objectUriInfoMap.get(HEALTH_CHECK);
        return ( (healthCheckStr != null) && healthCheckStr.equals("true") );
    }

    /*
     ** Return "tenancy-name" from the headers
     */
    public String getTenancy() {
        List<String> tenancyNames = headers.get(TENANCY_NAME);

        if ((tenancyNames == null) || (tenancyNames.size() != 1)) {
            return null;
        }

        return tenancyNames.get(0);
    }

    public String getCustomerName() {
        List<String> customerNames = headers.get(CUSTOMER_NAME);

        if ((customerNames == null) || (customerNames.size() != 1)) {
            return null;
        }

        return customerNames.get(0);
    }

    /*
    ** The accessToken is make up from the Tenancy, User Name and User Password to create a token that can be used to
    **   perform different methods. The current set of methods that use the accessToken are:
    **
    **     ObjectPut
    **     ObjectGet
    **     ObjectDelete
    **     ObjectList
    **     CreateBucket - POST
    **     DeleteBucket
    **     CreateNamespace
    **     DeleteNamespace
     */
    public String getAccessToken() {
        List<String> accessTokens = headers.get(ACCESS_TOKEN);

        if ((accessTokens == null) || (accessTokens.size() != 1)) {
            return null;
        }

        return accessTokens.get(0);
    }

    /*
    ** Return the "connection-key" that is used to validate the connection
     */
    public String getConnectionKey() {
        List<String> connectionKey = headers.get(CONNECTION_KEY);

        if ((connectionKey == null) || (connectionKey.size() != 1)) {
            return null;
        }

        return connectionKey.get(0);
    }

    public String getUserName() {
        List<String> userNames = headers.get(USER_NAME);

        if ((userNames == null) || (userNames.size() != 1)) {
            return null;
        }

        return userNames.get(0);
    }

    public String getUserPassword() {
        List<String> userPasswords = headers.get(USER_PASSWORD);

        if ((userPasswords == null) || (userPasswords.size() != 1)) {
            return null;
        }

        return userPasswords.get(0);
    }

    /*
    ** The following are used to pull specific fields from the headers that are part of the HTTP request or response
    **
    **     getOpcClientRequestId() - "opc-client-request-id"
    **     getOpcRequestId() - "opc-request-id"
    **     getContentLength() - "Content-Length" - but this is setup as a specific parsed field
    **     getContentMd5() - "Content-MD5"
    **     getMd5Override() - "md5-override"
    **
    **   The next three are specific the requests being sent to the Storage Server
    **     getObjectChunkNumber() - "object-chunk-number"
    **     getObjectChunkLba() - "chunk-lba"
    **     getObjectChunkLocation() - "chunk-location"
     */

    /*
     ** Return the "opc-client-request-id". This is an optional field in the PUT Object request.
     ** This field is provided by the client to allow them to track requests.
     */
    public String getOpcClientRequestId() {
        List<String> opcRequestId = headers.get(CLIENT_OPC_REQUEST_ID);

        if ((opcRequestId == null) || (opcRequestId.size() != 1)) {
            return null;
        }

        return opcRequestId.get(0);
    }

    public int getRequestId() { return requestId; }

    /*
     ** Return the "opc-request-id". This is an required field in the Storage Server PUT Object request.
     ** This field is generated by the Object Server when a request is received. It is guaranteed to be unique per
     **   request. A single request in the Object Server may fan out into multiple requests to Storage Servers.
     */
    public String getOpcRequestId() {
        List<String> opcRequestId = headers.get(OPC_REQUEST_ID);

        if ((opcRequestId == null) || (opcRequestId.size() != 1)) {
            return null;
        }

        return opcRequestId.get(0);
    }

    /*
     **
     */
    public int getContentLength() {
        if (contentLengthReceived) {
            return contentLength;
        }

        return -1;
    }

    public String getContentMd5 () {
        String md5 = getHeaderString(CONTENT_MD5);
        if ((md5 == null) || md5.isEmpty()) {
            md5 = "NULL";
        }

        return md5;
    }

    public boolean getMd5Override() {
        String md5Override = getHeaderString(MD5_OVERRIDE_HEADER);
        return ((md5Override != null) && md5Override.matches("true"));
    }

    public String getContentSha256 () {
        String sha256 = getHeaderString(CONTENT_SHA256);
        if (sha256.isEmpty()) {
            sha256 = null;
        }

        return sha256;
    }


    /*
    ** Will return -1 if the passed in "object-chunk-number" is not a valid representation of an integer or it is
    **   missing.
     */
    public int getObjectChunkNumber() {
        List<String> chunkNumbers = headers.get(CHUNK_NUMBER);

        if (chunkNumbers.size() != 1) {
            return -1;
        }

        int chunkNumber;
        try {
            chunkNumber = Integer.parseInt(chunkNumbers.get(0));
        } catch (NumberFormatException ex) {
            LOG.warn("chunk-id is not a valid numeric format - " + chunkNumbers.get(0));
            chunkNumber = -1;
        }

        return chunkNumber;
    }

    /*
     ** Will return -1 if the passed in "chunk-id" is not a valid representation of an integer or it is
     **   missing.
     */
    public int getObjectChunkId() {
        List<String> chunkIds = headers.get(CHUNK_ID);

        if (chunkIds.size() != 1) {
            return -1;
        }

        int chunkId;
        try {
            chunkId = Integer.parseInt(chunkIds.get(0));
        } catch (NumberFormatException ex) {
            LOG.warn("object-chunk-number is not a valid numeric format - " + chunkIds.get(0));
            chunkId = -1;
        }

        return chunkId;
    }

    /*
     ** Will return -1 if the passed in "chunk-lba" is not a valid representation of an integer or it is
     **   missing.
     */
    public int getObjectChunkLba() {
        List<String> chunkLbaHeaders = headers.get(CHUNK_LBA);

        if (chunkLbaHeaders.size() != 1) {
            return -1;
        }

        int chunkLba;
        try {
            chunkLba = Integer.parseInt(chunkLbaHeaders.get(0));
        } catch (NumberFormatException ex) {
            LOG.warn("object-chunk-number is not a valid numeric format - " + chunkLbaHeaders.get(0));
            chunkLba = -1;
        }

        return chunkLba;
    }

    /*
    ** Returns the location String if it is a valid string, otherwise it return null
     */
    public String getObjectChunkLocation() {
        List<String> chunkLocation = headers.get(CHUNK_LOCATION);

        if ((chunkLocation == null) || (chunkLocation.size() != 1)) {
            return null;
        }

        /*
        ** Validate the format of the location to insure it is only valid characters (letter, numbers, '_',
        **   '-', '/', and '.')
         */
        String location = chunkLocation.get(0);
        if (!location.matches("^[a-zA-Z0-9-_./]*$")) {
            location = null;
        }
        return location;
    }

    /*
    ** Return the "if-match" UID - This must be a valid UID (meaning it is 36 characters)
     */
    public String getIfMatchUid() {
        String uid = getHeaderString(IF_MATCH);
        if ((uid != null) && (uid.length() != 36)) {
            uid = null;
        }

        return uid;
    }

    /*
     ** Return the "versionId". Validate that it is a number
     */
    public int getVersionId() {
        String versionId = getHeaderString(VERSION_ID);
        int id = -1;

        if (versionId != null) {
            try {
                id = Integer.parseInt(versionId);
            } catch (NumberFormatException ex) {
                LOG.warn("Invalid versionId: " + versionId);
            }
        }

        return id;
    }


    /*
     ** Returns if the "if-none-match" is set - If this is present, the only valid value is '*'.
     **
     ** NOTE: This will also return false if the "if-match" is not set with a valid UID
     */
    public boolean getIfNoneMatch() {
        boolean ifNoneMatchSet;

        /*
        ** Make sure that there is a valid "if-match" header
         */
        String uid = getIfMatchUid();

        String ifNoneMatch = getHeaderString(IF_NONE_MATCH);
        if ((uid != null) && (ifNoneMatch != null) && ifNoneMatch.equals("*")) {
            ifNoneMatchSet = true;
        } else {
            ifNoneMatchSet = false;
        }

        return ifNoneMatchSet;
    }

    /*
     ** Returns if the service is supposed to be disabled
     **
     ** NOTE: For the health check, the service requires an explicit disable/enable operation. Simply not sending the
     **   "disable-service" or setting "disable-service" to false is not sufficient to enable the service after it has
     **   been disabled.
     */
    public boolean getServiceDisable() {
        List<String> disableService = headers.get(DISABLE_SERVICE);

        if ((disableService == null) || (disableService.size() != 1)) {
            return false;
        }

        /*
         */
        boolean disabled = false;
        String disabledStr = disableService.get(0);
        if (disabledStr.equalsIgnoreCase("true")) {
            disabled = true;
        }
        return disabled;
    }

    /*
     ** Returns if the service is supposed to be enabled
     **
     ** NOTE: For the health check, the service requires an explicit disable/enable operation. Simply not sending the
     **   "disable-service" or setting "disable-service" to false is not sufficient to enable the service after it has
     **   been disabled.
     */
    public boolean getServiceEnable() {
        List<String> enableService = headers.get(DISABLE_SERVICE);

        if ((enableService == null) || (enableService.size() != 1)) {
            return false;
        }

        /*
         */
        boolean enabled = false;
        String disabledStr = enableService.get(0);
        if (disabledStr.equalsIgnoreCase("true")) {
            enabled = true;
        }
        return enabled;
    }

    /*
     ** This finds all the occurrences of a passed in String in the headers key fields and adds those to the return
     **   string.
     */
    protected String getHeaderString(String name) {
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

    /*
    ** Return the list of "fields"
     */
    public void getFields(List<String> requestedFields) {
        List<String> values = headers.get(FIELDS_LIST);
        if ((values == null) || values.isEmpty()) {
            requestedFields.add("name");
            requestedFields.add("size");
            requestedFields.add("time-created");
            requestedFields.add("md5");
        } else {
            parseOutStrings(values, requestedFields);
        }
    }

    /*
    **
     */
    private void parseOutStrings(List<String> values, List<String> output) {
        for (String tmpStr: values) {
            /*
             ** First tokenize the String by ','
             */
            StringTokenizer stk = new StringTokenizer(tmpStr, " ,");
            while (stk.hasMoreTokens()) {
                String str1 = stk.nextToken();
                output.add(str1);
            }
        }
    }

    /*
    ** This provides a "standard" way to convert a String's content into a ByteBuffer that can be sent using the NIO
    **   calls
     */
    public static void str_to_bb(ByteBuffer out, final String in) {
        Charset charset = StandardCharsets.UTF_8;
        CharsetEncoder encoder = charset.newEncoder();

        try {
            encoder.encode(CharBuffer.wrap(in), out, true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
     ** Convert a ByteBuffer to a String
     */
    public static String bb_to_str(ByteBuffer buffer) {
        int position = buffer.position();
        String tmp = StandardCharsets.UTF_8.decode(buffer).toString();

        buffer.position(position);
        return tmp;
    }

}