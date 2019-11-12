package com.oracle.pic.casper.webserver.api.swift;

import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.WSExceptionTranslator;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringEscapeUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SwiftExceptionTranslator implements WSExceptionTranslator {

    private static final String SWIFT_ERROR_CONTENT_TYPE = "text/html; charset=UTF-8";

    @Override
    public Throwable rewriteException(RoutingContext context, @Nullable Throwable throwable) {
        if (throwable == null) {
            return new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR,
                "Context failed without throwable; see status code", context.request().path());
        }
        return SwiftHttpExceptionHelpers.rewrite(context, throwable);
    }

    @Override
    public int getHttpResponseCode(@Nonnull Throwable throwable) {
        if (throwable instanceof HttpException) {
            final HttpException hex = (HttpException) throwable;
            return hex.getErrorCode().getStatusCode();
        }
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;

    }

    @Override
    public String getResponseErrorContentType() {
        return SWIFT_ERROR_CONTENT_TYPE;
    }

    @Override
    public String getResponseErrorMessage(@Nonnull Throwable throwable) {
        if (throwable instanceof HttpException) {
            final HttpException hex = (HttpException) throwable;
            // Escape for HTML because Swift returns HTML error messages.
            String errorMessage = StringEscapeUtils.escapeHtml4(hex.getMessage());
            if (!errorMessage.equals(hex.getMessage())) {
                WebServerMetrics.SWIFT_XSS_POTENTIAL_ATTEMPTS.mark();
            }

            return errorMessage;
        } else {
            return "Internal server error";
        }
    }

    @Override
    public ErrorCode getErrorCode(@Nonnull Throwable throwable) {
        if (throwable instanceof HttpException) {
            final HttpException hex = (HttpException) throwable;
            return hex.getErrorCode();
        }
        return V2ErrorCode.INTERNAL_SERVER_ERROR;
    }

    @Override
    public void writeResponseErrorHeaders(@Nonnull RoutingContext routingContext, int errorCode) {
        final HttpServerResponse response = routingContext.response();
        final HttpServerRequest request = routingContext.request();

        SwiftHttpHeaderHelpers.writeCommonHeaders(routingContext);
        if (errorCode == 401) {
            final String accountName = SwiftHttpPathHelpers.getNamespace(request, WSRequestContext.get(routingContext));
            response.putHeader("WWW-Authenticate", "Casper realm=\"" + accountName + "\"");
        }
    }
}
