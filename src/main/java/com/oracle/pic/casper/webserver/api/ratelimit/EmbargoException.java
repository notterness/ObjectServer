package com.oracle.pic.casper.webserver.api.ratelimit;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;
import com.oracle.pic.casper.common.exceptions.ExceptionHttpStatus;

@ExceptionHttpStatus(value = 503, statusMessage = "ServiceUnavailable")
public class EmbargoException extends RuntimeException implements CasperHttpException {
    public EmbargoException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.SERVICE_UNAVAILABLE;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.SERVICE_UNAVAILABLE;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
