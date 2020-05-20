package com.webutils.webserver.common;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class Sha256ResultHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Sha256ResultHandler.class);

    private final RequestContext requestContext;
    private final HttpInfo httpInfo;

    private final AtomicBoolean sha256Parsed;

    private final AtomicBoolean sha256DigestComplete;
    private boolean contentHasValidSha256Digest;

    /*
     ** This is the value parsed out of the HTTP Request "x-content-sha256" header
     */
    private String sha256FromHeader;

    /*
     ** This is the value that was computed from the Http Request content
     */
    private String computedSha256Digest;


    public Sha256ResultHandler(final RequestContext requestContext, final HttpInfo httpInfo) {

        this.requestContext = requestContext;
        this.httpInfo = httpInfo;

        sha256Parsed = new AtomicBoolean(false);
        sha256DigestComplete = new AtomicBoolean(false);

        contentHasValidSha256Digest = false;
    }

    /*
     ** This extracts the expected Sha256 digest from the headers if it exists and then it validates that
     **   it is the correct length.
     ** Assuming it is found and the correct length it is then returned.
     */
    private String validateSha256Header() {
        sha256FromHeader = httpInfo.getContentSha256();
        if (sha256FromHeader == null) {
            sha256Parsed.set(false);
            return null;
        }

        sha256Parsed.set(true);
        return sha256FromHeader;
    }


    /**
     * Performs an integrity check on the body of an HTTP request if the "x-content-sha256" header is available.
     *
     * If "x-content-sha256" is not present, this function does nothing, otherwise it computes the sha256 value for
     *   the body and compares it to the value from the header.
     */
    public boolean checkContentSha256() {

        if (!sha256Parsed.get())
        {
            LOG.warn("checkContentSha256() [" + requestContext.getRequestId() + "] sha256parsed: " + sha256Parsed.get());
            contentHasValidSha256Digest = true;
            return true;
        }

        if (sha256FromHeader != null) {
            if (!sha256FromHeader.equals(computedSha256Digest)) {
                LOG.warn("x-content-sha256 [" + requestContext.getRequestId() +  "] did not match computed. expected: " +
                        sha256FromHeader + " computed: " + computedSha256Digest);

                String failureMessage = "\"Bad Sha256 Compare\",\n  \"x-content-sha256\": \"" + sha256FromHeader +
                        "\",\n  \"Computed Sha256\": \"" + computedSha256Digest + "\"";

                httpInfo.setParseFailureCode(HttpStatus.UNPROCESSABLE_ENTITY_422, failureMessage);
                return false;
            }
        } else {
            LOG.warn("x-content-sha256 [" + requestContext.getRequestId() +  "] passed in was invalid. computed: " +
                    computedSha256Digest);
            String failureMessage = "\"Invalid or missing x-content-sha256\",\n  \"Computed Sha256\": \"" + computedSha256Digest + "\"";

            httpInfo.setParseFailureCode(HttpStatus.BAD_REQUEST_400, failureMessage);
            return false;
        }

        LOG.warn("checkContentSha256() [" + requestContext.getRequestId() +  "] passed");
        contentHasValidSha256Digest = true;
        return true;
    }

    /*
     ** Accessor methods for the Sha256 Digest information
     **
     ** NOTE: The setSha256DigestComplete() method is called from a compute thread that is not the same as the worker
     **   threads.
     **       The getSha256DigestComplete() is called from a worker thread. It must be true before the worker thread
     **   accesses the getSha256CompareValid() and the getComputedSha256Digest() methods so it acts as a barrier to insure
     **   that the contentHasValidSha256Digest and computedSha256Digest values are valid.
     */
    public void setSha256DigestComplete(final String digest) {

        LOG.info("setSha256DigestComplete() " + digest);

        computedSha256Digest = digest;
        sha256DigestComplete.set(true);
    }

    public boolean getSha256DigestComplete() { return sha256DigestComplete.get(); }

    public String getComputedSha256Digest() {
        if (sha256DigestComplete.get()) {
            return computedSha256Digest;
        }

        return null;
    }

    public boolean getSha256CompareValid() { return contentHasValidSha256Digest; }

}
