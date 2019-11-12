package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.error.ErrorCode;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface WSExceptionTranslator {

    /**
     * Attempt to rewrite the exception that failed the request to something that we can work with for HTTP
     *
     * @param throwable raw throwable that failed the request, possibly wrapped in CompletionExceptions, or null
     * @return an unwrapped throwable, possibly rewritten as a custom API-specific HTTP exception, or a generic
     *         Internal Server Error appropriate for the API.
     */
    Throwable rewriteException(RoutingContext countext, @Nullable Throwable throwable);

    /**
     * Get the HTTP response status code.
     *
     * @param throwable  unwrapped throwable that failed the request
     */
    int getHttpResponseCode(@Nonnull Throwable throwable);

    /**
     * Get the Content-Type of the response error message.
     */
    String getResponseErrorContentType();

    /**
     * Get the error message that should be returned to the client for this type of exception.
     *
     * Note:  HEAD requests may not return bodies, so this method will not be called for HEAD requests.
     *
     * @param throwable  unwrapped throwable that failed the request
     */
    String getResponseErrorMessage(@Nonnull Throwable throwable);

    /**
     * Write any additional headers that should be included on error responses.
     *
     * @param routingContext  the routing context for the request, allowing access to everything we know about it
     * @param errorCode       the HTTP response status code
     */
    // This will probably need to take the Throwable as an argument eventually, but right now no implementation needs it
    void writeResponseErrorHeaders(@Nonnull RoutingContext routingContext, int errorCode);

    /**
     * Get the error code associated with this exception.
     *
     * This code is currently used for public logging purposes
     *
     * @param throwable unwrapped throwable that failed the request
     * @return
     */
    ErrorCode getErrorCode(@Nonnull Throwable throwable);
}
