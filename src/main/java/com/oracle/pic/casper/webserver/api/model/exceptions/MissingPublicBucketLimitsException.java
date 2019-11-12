package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class MissingPublicBucketLimitsException extends RuntimeException implements CasperHttpException {
    public MissingPublicBucketLimitsException(String msg) {
        super(msg);
    }

    public MissingPublicBucketLimitsException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.UNAUTHORIZED;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.ACCESS_DENIED;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
