package com.oracle.pic.casper.webserver.api.common;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;
import com.oracle.pic.casper.common.util.ThrowableUtil;
import io.vertx.core.http.HttpServerRequest;
import org.apache.commons.lang.NotImplementedException;

import javax.annotation.Nullable;
import java.util.concurrent.CompletionException;

/**
 * Http exception contains an error code, and the resource path.
 * It is used for returning error response to http requests.
 */
public final class HttpException extends RuntimeException {
    private final ErrorCode errorCode;
    private final String resourcePath;

    public HttpException(ErrorCode errorCode, String message, String resourcePath, @Nullable Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.resourcePath = resourcePath;
    }

    public HttpException(ErrorCode errorCode, String message, String resourcePath) {
        this(errorCode, message, resourcePath, null);
    }

    public HttpException(ErrorCode errorCode, String resourcePath, Throwable cause) {
        this(errorCode, cause.getMessage(), resourcePath, cause);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public CharSequence getResourcePath() {
        return resourcePath;
    }

    public static <T> T handle(HttpServerRequest request, @Nullable T val, Throwable thrown) {
        if (thrown == null) {
            return val;
        }
        throw rewrite(request, thrown);
    }

    public static RuntimeException rewrite(HttpServerRequest request, Throwable thrown) {
        thrown = ThrowableUtil.getUnderlyingThrowable(Preconditions.checkNotNull(thrown));
        if (thrown instanceof HttpException) {
            return (HttpException) thrown;
        }

        final String path = request.path();

        if (thrown instanceof CasperHttpException) {
            CasperHttpException casperHttpException = (CasperHttpException) thrown;
            return new HttpException(casperHttpException.errorCode(), casperHttpException.errorMessage(), path,
                thrown);
        }

        if (thrown instanceof NotImplementedException) {
            return new HttpException(V2ErrorCode.NOT_IMPLEMENTED, thrown.getMessage(), path, thrown);
        }

        if (thrown instanceof UnrecognizedPropertyException) {
            return new HttpException(V2ErrorCode.INVALID_JSON, thrown.getMessage(), path, thrown);
        }

        return new CompletionException(thrown);
    }
}
