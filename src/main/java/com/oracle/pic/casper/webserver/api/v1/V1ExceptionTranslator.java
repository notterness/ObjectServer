package com.oracle.pic.casper.webserver.api.v1;

import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.ExceptionHttpStatus;
import com.oracle.pic.casper.common.exceptions.ExceptionTranslator;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.ThrowableUtil;
import com.oracle.pic.casper.common.vertx.ServerExceptionTranslator;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.WSExceptionTranslator;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class V1ExceptionTranslator implements WSExceptionTranslator {

    @Override
    public Throwable rewriteException(RoutingContext context, @Nullable Throwable throwable) {
        if (throwable == null) {
            return new InternalServerErrorException("Context failed without throwable; see status code");
        }
        return throwable;
    }

    @Override
    public int getHttpResponseCode(@Nonnull Throwable throwable) {
        final Throwable rootCause = ThrowableUtil.getUnderlyingThrowable(throwable);
        if (rootCause instanceof HttpException) {
            return ((HttpException) rootCause).getErrorCode().getStatusCode();
        }
        final ExceptionHttpStatus exceptionHttpStatus = ExceptionTranslator.getExceptionHttpStatus(rootCause);
        if (exceptionHttpStatus != null) {
            return exceptionHttpStatus.value();
        } else {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }

    @Override
    public String getResponseErrorContentType() {
        return ContentType.APPLICATION_JSON;
    }

    @Override
    public String getResponseErrorMessage(@Nonnull Throwable throwable) {
        final Throwable rootCause = ThrowableUtil.getUnderlyingThrowable(throwable);
        if (rootCause instanceof HttpException) {
            HttpException httpException = (HttpException) rootCause;
            final String statusMessage = httpException.getMessage();
            return ServerExceptionTranslator.getErrorMessageDetail(throwable, rootCause, statusMessage);
        }

        String statusMessage = null;
        final ExceptionHttpStatus exceptionHttpStatus = ExceptionTranslator.getExceptionHttpStatus(rootCause);
        if (exceptionHttpStatus != null) {
            statusMessage = exceptionHttpStatus.statusMessage();
        }

        return ServerExceptionTranslator.getErrorMessageDetail(throwable, rootCause, statusMessage);
    }

    @Override
    public ErrorCode getErrorCode(@Nonnull Throwable throwable) {
        final Throwable rootCause = ThrowableUtil.getUnderlyingThrowable(throwable);
        if (rootCause instanceof HttpException) {
            HttpException httpException = (HttpException) rootCause;
            return httpException.getErrorCode();
        }
        return V2ErrorCode.INTERNAL_SERVER_ERROR;
    }

    @Override
    public void writeResponseErrorHeaders(@Nonnull RoutingContext routingContext, int errorCode) {
        final HttpServerResponse response = routingContext.response();

        if (errorCode == HttpResponseStatus.UNAUTHORIZED) {
            response.putHeader("WWW-Authenticate", "Casper realm=\"AUTH_casper\"");
        }
    }
}
