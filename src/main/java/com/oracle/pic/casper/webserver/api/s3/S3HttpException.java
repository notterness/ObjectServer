package com.oracle.pic.casper.webserver.api.s3;

import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;
import com.oracle.pic.casper.common.util.ThrowableUtil;
import com.oracle.pic.casper.webserver.auth.dataplane.exceptions.IncorrectRegionException;
import com.oracle.pic.casper.webserver.api.s3.model.S3Error;
import com.oracle.pic.casper.webserver.api.s3.model.S3IncorrectRegionError;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.NotImplementedException;

import javax.annotation.Nullable;

public final class S3HttpException extends RuntimeException {
    private final S3Error error;
    private final String resourcePath;

    public S3HttpException(ErrorCode errorCode, String message, String resourcePath, @Nullable Throwable cause) {
        super(cause);
        this.error = new S3Error(errorCode, message);
        this.resourcePath = resourcePath;
    }

    public S3HttpException(ErrorCode errorCode, String message, String resourcePath) {
        this(errorCode, message, resourcePath, null);
    }

    public S3HttpException(S3Error error, String resourcePath) {
        this.error = error;
        this.resourcePath = resourcePath;
    }

    public S3Error getError() {
        return error;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public static RuntimeException rewrite(RoutingContext context, Throwable thrown) {
        thrown = ThrowableUtil.getUnderlyingThrowable(Preconditions.checkNotNull(thrown));

        if (thrown instanceof S3HttpException) {
            return (S3HttpException) thrown;
        }

        final String path = context.request().path();

        if (thrown instanceof CasperHttpException) {
            CasperHttpException casperHttpException = (CasperHttpException) thrown;
            return new S3HttpException(casperHttpException.s3ErrorCode(), casperHttpException.errorMessage(), path,
                    thrown);
        }

        if (thrown instanceof NotImplementedException) {
            return new S3HttpException(S3ErrorCode.NOT_IMPLEMENTED, thrown.getMessage(), path, thrown);
        }

        if (thrown instanceof IncorrectRegionException) {
            IncorrectRegionException incorrectRegionException = (IncorrectRegionException) thrown;
            return new S3HttpException(new S3IncorrectRegionError(WSRequestContext.getCommonRequestContext(context)
                    .getOpcRequestId(), context.request().host(), incorrectRegionException.getExpectedRegion(),
                    thrown.getMessage()), path);
        }

        return new S3HttpException(S3ErrorCode.INTERNAL_ERROR, "We encountered an internal error. Please try again.",
            path, thrown);
    }
}
