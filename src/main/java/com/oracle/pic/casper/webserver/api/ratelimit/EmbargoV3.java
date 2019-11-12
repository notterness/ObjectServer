package com.oracle.pic.casper.webserver.api.ratelimit;

/**
 * Embargo provides a way for ad-hoc blocking or rate-limiting of calls made
 * to Casper. Each handler should call {@link #enter(EmbargoV3Operation)} before
 * doing any work, to see if the operation is allowed.
 */
public interface EmbargoV3 {

    /**
     * Call when we are about to start an operation. This will throw an
     * exception if the operation should not be allowed.
     */
    void enter(EmbargoV3Operation operation);
}
