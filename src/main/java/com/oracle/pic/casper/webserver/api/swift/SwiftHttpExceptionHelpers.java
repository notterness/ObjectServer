package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.InvalidDigestException;
import com.oracle.pic.casper.common.util.ThrowableUtil;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.model.exceptions.BucketAlreadyExistsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchObjectException;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class SwiftHttpExceptionHelpers {

    private static final String NOT_FOUND_MSG = "<html><h1>Not Found</h1>" +
        "<p>The resource could not be found.</p></html>";

    private SwiftHttpExceptionHelpers() {
    }

    public static <T> T handle(RoutingContext context, @Nullable T val, Throwable thrown) {
        if (thrown == null) {
            return val;
        }
        throw rewrite(context, thrown);
    }

    /**
     * Filter API exceptions for Swift-specific HTTP exceptions.
     *
     * This method wraps the eponymous method in {@link com.oracle.pic.casper.webserver.api.common.HttpException},
     * simply adding an additional filtering layer before it.  This allows us to throw different HttpExceptions than
     * what v2 would throw, as required occasionally by Swift.  Known differences:
     *
     * * On a PUT object request, if the client sent an MD5 and the data stream doesn't hash to it, our backend will
     *   raise {@link com.oracle.pic.casper.common.exceptions.InvalidDigestException}.  V2 treats this as a 400 Bad
     *   Request, but Swift returns 422 Unprocessable Entity.  V2 also treats undecodeable MD5s as 400s, but Swift
     *   treats them all as 422s.
     *
     * * In many cases, the v2 and Swift APIs both return the same error code, but the response bodies will be
     *   different, with different error messages (for example NotAuthorizedException).  For now, we don't actually
     *   rewrite those error messages.
     */
    public static RuntimeException rewrite(RoutingContext context, @Nonnull Throwable throwable) {
        Throwable rootCause = ThrowableUtil.getUnderlyingThrowable(throwable);

        final String path = context.request().path();

        if (rootCause instanceof NoSuchObjectException) {
            return new HttpException(V2ErrorCode.OBJECT_NOT_FOUND, NOT_FOUND_MSG, path, rootCause);
        }

        if (rootCause instanceof InvalidDigestException) {
            return new HttpException(V2ErrorCode.UNMATCHED_ETAG_MD5,
                "Valid ETag did not match calculated data stream MD5", path, rootCause);
        }

        if (rootCause instanceof BucketAlreadyExistsException) {
            throw new RuntimeException(
                "FIXME!  This message should never be reached.  If it is, then we've added " +
                    "support for Swift's create container API call without handling the bucket-already-exists case. " +
                    "V2 throws 400 in this case, but Swift treats container creation as idempotent and returns 202.",
                throwable
            );
        }

        return HttpException.rewrite(context.request(), rootCause);
    }
}
