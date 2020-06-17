package com.webutils.webserver.common;

import com.google.common.io.BaseEncoding;
import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class Md5ResultHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Md5ResultHandler.class);

    protected final RequestContext requestContext;
    private final HttpInfo httpInfo;

    private final AtomicBoolean md5Parsed;
    private boolean md5Override;

    protected final AtomicBoolean md5DigestComplete;
    private boolean contentHasValidMd5Digest;

    /*
    ** This is the value parsed out of the HTTP Request "Content-Md5" header
     */
    protected String md5FromHeader;

    /*
    ** This is the value that was computed from the Http Request content
     */
    protected String computedMd5Digest;


    public Md5ResultHandler(final RequestContext requestContext, final HttpInfo httpInfo) {

        this.requestContext = requestContext;
        this.httpInfo = httpInfo;

        md5Parsed = new AtomicBoolean(false);
        md5DigestComplete = new AtomicBoolean(false);

        contentHasValidMd5Digest = false;
        md5Override = false;
    }

    /*
     ** This extracts the expected MD5 digest from the headers if it exists and then it validates that
     **   it is the correct length.
     ** Assuming it is found and the correct length it is then returned.
     */
    public void validateMD5Header() {
        md5FromHeader = httpInfo.getContentMd5();
        if (md5FromHeader == null) {
            LOG.warn("Content-MD5 header not provided");

            md5Parsed.set(false);
            return;
        }

        md5Override = httpInfo.getMd5Override();

        try {
            byte[] bytes = BaseEncoding.base64().decode(md5FromHeader);
            if (bytes.length != 16) {
                LOG.warn("The value of the Content-MD5 header '" + md5FromHeader +
                        "' was not the correct length after base-64 decoding");
                md5FromHeader = null;
                md5Parsed.set(false);
                return;
            }
        } catch (IllegalArgumentException ia_ex) {
            LOG.warn("The value of the Content-MD5 header '" + md5FromHeader +
                    "' was not the correct length after base-64 decoding");
            md5FromHeader = null;
            md5Parsed.set(false);
            return;
        }

        md5Parsed.set(true);
    }


    /**
     * Performs an integrity check on the body of an HTTP request if the Content-MD5 header is available.
     *
     * If Content-MD5 is not present, this function does nothing, otherwise it computes the MD5 value for the body and
     * compares it to the value from the header.
     */
    public void checkContentMD5() {
        if (!md5Parsed.get() || md5Override)
        {
            LOG.warn("checkContentMd5() [" + requestContext.getRequestId() + "] md5parsed: " + md5Parsed.get() +
                    " md5Override: " + md5Override);
            contentHasValidMd5Digest = true;
            return;
        }

        if (!md5DigestComplete.get()) {
            LOG.warn("Content-MD5 [" + requestContext.getRequestId() + "] no computed Md5 digest: " + md5FromHeader);

            String failureMessage = "\"No computed Md5 digest\",\n  \"Content-MD5\": \"" + md5FromHeader +
                    "\"";

            httpInfo.setParseFailureCode(HttpStatus.UNPROCESSABLE_ENTITY_422, failureMessage);
            return;
        }

        if (md5FromHeader != null) {
            if (!md5FromHeader.equals(computedMd5Digest)) {
                LOG.warn("Content-MD5 [" + requestContext.getRequestId() + "] did not match computed. expected: " +
                        md5FromHeader + " computed: " + computedMd5Digest);

                String failureMessage = "{\r\n  \"code\": " + HttpStatus.UNPROCESSABLE_ENTITY_422 +
                        "\r\n  \"message\": \"Failed Md5 Compare\"" +
                        "\r\n  \"Content-MD5\": \"" + md5FromHeader + "\"" +
                        "\r\n  \"Computed-MD5\": \"" + computedMd5Digest + "\"" +
                        "\r\n}";

                httpInfo.setParseFailureCode(HttpStatus.UNPROCESSABLE_ENTITY_422, failureMessage);
                return;
            }
        } else {
            LOG.warn("Content-MD5 [" + requestContext.getRequestId() + "] passed in was invalid. computed: " +
                    computedMd5Digest);
            String failureMessage = "{\r\n  \"code\": " + HttpStatus.BAD_REQUEST_400 +
                    "\r\n  \"message\": \"Invalid or missing Content-Md5\"" +
                    "\r\n  \"Computed-MD5\": \"" + computedMd5Digest + "\"" +
                    "\r\n}";

            httpInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
            return;
        }

        LOG.warn("checkContentMd5() [" + requestContext.getRequestId() + "] passed");
        contentHasValidMd5Digest = true;
    }

    /*
     ** Accessor methods for the Md5 Digest information
     **
     ** NOTE: The setDigestComplete() method is called from a compute thread that is not the same as the worker
     **   threads.
     **       The getMd5DigestComplete() is called from a worker thread. It must be true before the worker thread
     **   accesses the getMd5CompareValid() and the getComputedMd5Digest() methods so it acts as a barrier to insure
     **   that the contentHasValidMd5Digest and computedMd5Digest values are valid.
     */
    public void setMd5DigestComplete(final String digest) {
        LOG.info("setMd5DigestComplete() digest: " + digest);

        computedMd5Digest = digest;
        md5DigestComplete.set(true);
    }

    public boolean getMd5DigestComplete() {
        boolean complete = md5DigestComplete.get();

        LOG.info("getMd5DigestComplete() " + complete);

        return complete;
    }

    public String getComputedMd5Digest() {
        if (md5DigestComplete.get()) {
            return computedMd5Digest;
        }

        return null;
    }

    public boolean getMd5CompareValid() { return contentHasValidMd5Digest; }
}
