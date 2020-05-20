package com.webutils.webserver.http;

import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.BadMessageException;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class HttpResponseInfo extends HttpInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HttpResponseInfo.class);

    private static final String OPC_CONTENT_MD5 = "opc-content-md5";
    private static final String RESPONSE_HEADER_ETAG = "etag";
    private static final String RESPONSE_LAST_MODIFIED = "last-modified";
    private static final String RESPONSE_VERSION_ID = "version-id";

    private static final String RESPONSE_ALLOWED_METHODS = "allow";

    private final AtomicBoolean statusSet;

    public HttpResponseInfo(final int requestTrackingId) {

        super();

        statusSet = new AtomicBoolean(false);
        requestId = requestTrackingId;
    }

    /*
     ** The HTTP response passes back the Md5 Digest in the "opc-content-md5" header instead of
     **   the "Content-MD5" header.
     */
    public String getResponseContentMd5() {
        String md5 = getHeaderString(OPC_CONTENT_MD5);
        if ((md5 != null) && md5.isEmpty()) {
            md5 = null;
        }

        return md5;
    }

    /*
     ** The HTTP response passes back the ETag in the "etag" header
     */
    public String getResponseEtag() {
        String etag = getHeaderString(RESPONSE_HEADER_ETAG);
        if ((etag != null) && etag.isEmpty()) {
            etag = null;
        }

        return etag;
    }

    /*
     ** The HTTP response passes back the last modified in the "last-modified" header
     */
    public String getResponseLastModified() {
        String lastModified = getHeaderString(RESPONSE_LAST_MODIFIED);
        if ((lastModified != null) && lastModified.isEmpty()) {
            lastModified = null;
        }

        return lastModified;
    }

    /*
    **
     */
    public String getAllowableMethods() {
        String allowableMethods = getHeaderString(RESPONSE_ALLOWED_METHODS);
        if ((allowableMethods != null) && allowableMethods.isEmpty()) {
            allowableMethods = null;
        }

        return allowableMethods;
    }

    /*
     ** The HTTP response passes back the object version Id in the "version-id" header
     */
    public String getResponseVersionId() {
        String versionId = getHeaderString(RESPONSE_VERSION_ID);
        if ((versionId != null) && versionId.isEmpty()) {
            versionId = null;
        }

        return versionId;
    }


    public synchronized void setResponseStatus(final int status) {
        LOG.info("setResponseStatus() status: " + status);

        parseFailureCode = status;
        statusSet.set(true);
    }

    public synchronized int getResponseStatus() {
        if (statusSet.get()) {
            return parseFailureCode;
        }

        return -1;
    }

    public void setResponseBody(final String responseError) {
        parseFailureReason = responseError;
    }

    public String getResponseBody() {
        return parseFailureReason;
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

            LOG.warn("No Content-Length [" + requestId + "] code: " +
                    parseFailureCode + " reason: " + parseFailureReason);
        }

        headerComplete = true;
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
                if ((contentLength < 0) || (contentLength > RequestContext.getChunkSize())) {
                    contentLength = 0;
                    parseFailureCode = HttpStatus.RANGE_NOT_SATISFIABLE_416;
                    parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                    LOG.info("Invalid Content-Length [" + requestId + "] code: " +
                            parseFailureCode + " reason: " + parseFailureReason);
                }
            } catch (NumberFormatException num_ex) {
                LOG.info("addHeaderValue() " + field.getName() + " " + num_ex.getMessage());

                contentLength = 0;
                parseFailureCode = HttpStatus.RANGE_NOT_SATISFIABLE_416;
                parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                LOG.info("Invalid Content-Length [" + requestId + "] code: " +
                        parseFailureCode + " reason: " + parseFailureReason);
            }
        }
    }

    public boolean getHttpParseError() {
        return (parseFailureCode != HttpStatus.OK_200);
    }

    public void httpHeaderError(final BadMessageException failure) {
        super.httpHeaderError(failure);
    }

}
