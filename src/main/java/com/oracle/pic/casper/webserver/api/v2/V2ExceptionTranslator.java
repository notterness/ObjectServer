package com.oracle.pic.casper.webserver.api.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.WSExceptionTranslator;
import com.oracle.pic.casper.webserver.api.model.ErrorInfo;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Writes out {@link HttpException} exceptions using {@link ErrorInfo} as the JSON format.
 *
 * Any exception that is not an {@link HttpException} is written out as an internal server error (status 500).
 */
public class V2ExceptionTranslator implements WSExceptionTranslator {
    private static final Logger LOG = LoggerFactory.getLogger(V2ExceptionTranslator.class);

    private final ObjectMapper mapper;

    public V2ExceptionTranslator(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Throwable rewriteException(RoutingContext context, @Nullable Throwable throwable) {
        if (throwable == null) {
            return new HttpException(V2ErrorCode.INTERNAL_SERVER_ERROR,
                "Context failed without throwable; see status code", context.request().path());
        }
        return HttpException.rewrite(context.request(), throwable);
    }

    @Override
    public int getHttpResponseCode(@Nonnull Throwable throwable) {
        if (throwable instanceof HttpException) {
            final HttpException hex = (HttpException) throwable;
            return hex.getErrorCode().getStatusCode();
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
        final ErrorInfo errorInfo;
        if (throwable instanceof HttpException) {
            final HttpException hex = (HttpException) throwable;
            errorInfo = new ErrorInfo(hex.getErrorCode().getErrorName(), hex.getMessage());
        } else {
            errorInfo = new ErrorInfo(V2ErrorCode.INTERNAL_SERVER_ERROR.getErrorName(), "Internal server error");
        }
        try {
            LOG.debug("ERROR: {}", mapper.writeValueAsString(errorInfo));
            return mapper.writeValueAsString(errorInfo);
        } catch (JsonProcessingException jpex) {
            LOG.warn("Failed to serialize error info as JSON: {}", errorInfo, jpex);
            return "";
        }
    }

    @Override
    public void writeResponseErrorHeaders(@Nonnull RoutingContext routingContext, int errorCode) {
        final HttpServerResponse response = routingContext.response();

        if (errorCode == HttpResponseStatus.UNAUTHORIZED) {
            response.putHeader("WWW-Authenticate", "Casper realm=\"AUTH_casper\"");
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
}
