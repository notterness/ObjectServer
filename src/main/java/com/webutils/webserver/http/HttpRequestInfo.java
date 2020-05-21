package com.webutils.webserver.http;

import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class HttpRequestInfo extends HttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestInfo.class);

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

    public HttpRequestInfo() {

        super();

        /*
        ** Set the objectId to -1 to indicate that it is not valid.
         */
        objectId = -1;
    }


    /*
     ** Clear out all of the String fields and release the memory
     */
    void reset() {

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
        return Objects.requireNonNullElse(successResponseHeaders, "");
    }

    public String getResponseContent() {
        return Objects.requireNonNullElse(successResponseContent, "");
    }

    /*
     ** When the headers have been completely read in, that will be the time to insure it is valid
     **   and the field values make sense.
     */
    public abstract void setHeaderComplete();

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

                    LOG.info("Invalid Content-Length [" + requestId +  "] code: " +
                            parseFailureCode + " reason: " + parseFailureReason);
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
    }

}
