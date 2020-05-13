package com.webutils.webserver.http;

import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class HttpRequestInfo extends HttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestInfo.class);

    /*
     ** This defines the redundancy and placement for the Objects within a bucket
     */
    private StorageTierEnum storageTier;

    /*
    ** The objectId is the identifier to access the Object record within the ObjectStorageDd after it has been created. It is
    **   generated during the INSERT operation on the table and uses an AUTO_INCREMENT field.
    ** The objectUID is String that represents the Object and is also generated during the INSERT operation using the
    **   UUID() MySQL function.
    **
    ** NOTE: These variables are only filled in for operations that operate on Objects
     */
    private int objectId;
    private String objectUID;

    private String successResponseContent;
    private String successResponseHeaders;

    /*
    ** This map is used to hold the Object Name, Bucket Name and Tenancy Name for the created objected.
    ** For Storage Servers, there is also a Test Type that is used to force certain behaviors in the
    **   Storage Server's responses (i.e. disconnect the connection).
     */
    private final Map<String, String> putObjectInfoMap;


    public HttpRequestInfo(final RequestContext requestContext) {

        super(requestContext);

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

        storageTier = StorageTierEnum.STANDARD_TIER;
        objectId = -1;
        objectUID = null;

        successResponseContent = null;
        successResponseHeaders = null;
    }


    public void setObjectId(final int id, final String uid) {
        objectId = id;
        objectUID = uid;
    }

    public int getObjectId() { return objectId; }

    public String getObjectUID() { return objectUID; }

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
            /* FIXME: Need to handle the differences between Object Server and Storage Server PUT required fields
            if ((getObject() == null) || (getBucket() == null) || (getNamespace() == null)) {
                parseFailureCode = HttpStatus.BAD_REQUEST_400;
                parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                LOG.warn("PUT Missing Critical Object Info [" + requestContext.getRequestId() + "] code: " +
                        parseFailureCode + " reason: " + parseFailureReason);

                requestContext.setHttpParsingError();
            }

             */
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

    /*
     ** This function will pull out the various information in the HTTP header fields and add it to
     ** the associated string within this object.
     */
    public void addHeaderValue(HttpField field) {
        super.addHeaderValue(field);

        /*
         ** The CONTENT_LENGTH is parsed out early as it is used in the headers parsed callback to
         **   setup the next stage of the connection pipeline.
         */
        final String fieldName = field.getName().toLowerCase();

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
    ** This needs to inform the RequestContext that there was a parsing error for the HTTP request.
     */
    public void httpHeaderError(final BadMessageException failure) {
        super.httpHeaderError(failure);

        requestContext.setHttpParsingError();
    }

}
