package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.webserver.api.common.WSExceptionTranslator;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

public class S3ExceptionTranslator implements WSExceptionTranslator {
    private static final Logger LOG = LoggerFactory.getLogger(S3ExceptionTranslator.class);
    private final XmlMapper mapper;

    public S3ExceptionTranslator(XmlMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Throwable rewriteException(RoutingContext context, @Nullable Throwable throwable) {
        if (throwable == null) {
            return new S3HttpException(S3ErrorCode.INTERNAL_ERROR,
                "We encountered an internal error. Please see status code.", context.request().path());
        }
        return S3HttpException.rewrite(context, throwable);
    }

    @Override
    public int getHttpResponseCode(@Nonnull Throwable throwable) {
        if (throwable instanceof S3HttpException) {
            final S3HttpException s3HttpException = (S3HttpException) throwable;
            return s3HttpException.getError().getErrorCode().getStatusCode();
        } else {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }
    }

    @Override
    public String getResponseErrorContentType() {
        return ContentType.APPLICATION_XML_UTF8;
    }

    @Override
    public String getResponseErrorMessage(@Nonnull Throwable throwable) {
        //we do not want error message for 304 Not Modified (or other non-errors)
        if (getHttpResponseCode(throwable) < 400) {
            return "";
        } else if (throwable instanceof S3HttpException) {
            S3HttpException s3HttpException = (S3HttpException) throwable;
            try {
                return mapper.writeValueAsString(s3HttpException.getError());
            } catch (IOException e) {
                LOG.error("Could not create http message for {}, got exception {}", throwable, e);
            }
        }
        return "Internal Server Error";
    }

    @Override
    public ErrorCode getErrorCode(@Nonnull Throwable throwable) {
        if (throwable instanceof S3HttpException) {
            final S3HttpException s3HttpException = (S3HttpException) throwable;
            return s3HttpException.getError().getErrorCode();
        }
        return S3ErrorCode.INTERNAL_ERROR;

    }

    @Override
    public void writeResponseErrorHeaders(@Nonnull RoutingContext routingContext, int errorCode) {
        // TODO: if the response doesn't already have the request id header, get it from the WSRequestContext and add
        // the s3 header.
    }
}
