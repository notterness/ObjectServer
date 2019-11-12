package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import io.vertx.core.http.HttpServerRequest;

import java.util.Optional;
import java.util.OptionalInt;

public final class SwiftHttpQueryHelpers {

    public static final int MAX_LIMIT = 10000;
    public static final String MARKER_PARAM = "marker";
    public static final String LIMIT_PARAM = "limit";

    private SwiftHttpQueryHelpers() {
    }

    /**
     * Gets the marker from a list request.  Markers are Swift's term for cursors, or pages
     *
     * @param request a request to list containers or objects
     * @return the marker on the request, or an empty Optional if no marker was found.
     */
    public static Optional<String> getMarker(HttpServerRequest request) {
        return Optional.ofNullable(request.getParam(MARKER_PARAM));
    }

    /**
     * Gets the maximum page size from a list request.
     *
     * @param request a request to list containers or objects
     * @return the client-specified maximum page size, or an empty Optional if the value was absent or
     * unparseable
     * @throws HttpException 412 Precondition Failed if the user requests too many records (> 10000)
     */
    public static OptionalInt getLimit(HttpServerRequest request) {
        String limitStr = request.getParam(LIMIT_PARAM);
        if (limitStr == null) {
            return OptionalInt.empty();
        }

        Integer limit;
        try {
            limit = Integer.parseInt(limitStr);
        } catch (NumberFormatException nfex) {
            // If you pass a string that isn't an integer into Swift, Swift ignores it.
            return OptionalInt.empty();
        }
        // Swift will return 412 Precondition Failed if you ask for > 10000 records in plaintext format, but it doesn't
        // fail if you ask it for JSON.  Is there a different limit, or is it just ignoring the limit?
        // For now, we won't return errors if clients set the limit too high; we already force it down.
        if (limit < 0) {
            // If you pass a negative limit into Swift, Swift ignores is.
            return OptionalInt.empty();
        }
        if (limit > MAX_LIMIT) {
            throw new HttpException(V2ErrorCode.LIMIT_TOO_HIGH, "Maximum limit is " + MAX_LIMIT, request.path());
        }
        return OptionalInt.of(limit);
    }
}
