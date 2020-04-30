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

    private final AtomicBoolean statusSet;
    private int responseStatus;

    public HttpResponseInfo(final RequestContext requestContext) {

        super(requestContext);

        statusSet = new AtomicBoolean(false);
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

    public synchronized void setResponseStatus(final int status) {
        responseStatus = status;
        statusSet.set(true);
    }

    public synchronized int getResponseStatus() {
        if (statusSet.get()) {
            return responseStatus;
        }

        return -1;
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

            LOG.warn("No Content-Length [" + requestContext.getRequestId() + "] code: " +
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
                if ((contentLength < 0) || (contentLength > RequestContext.getChunkSize())) {
                    contentLength = 0;
                    parseFailureCode = HttpStatus.RANGE_NOT_SATISFIABLE_416;
                    parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                    LOG.info("Invalid Content-Length [" + requestContext.getRequestId() + "] code: " +
                            parseFailureCode + " reason: " + parseFailureReason);
                }
            } catch (NumberFormatException num_ex) {
                LOG.info("addHeaderValue() " + field.getName() + " " + num_ex.getMessage());

                contentLength = 0;
                parseFailureCode = HttpStatus.RANGE_NOT_SATISFIABLE_416;
                parseFailureReason = HttpStatus.getMessage(parseFailureCode);

                LOG.info("Invalid Content-Length [" + requestContext.getRequestId() + "] code: " +
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
