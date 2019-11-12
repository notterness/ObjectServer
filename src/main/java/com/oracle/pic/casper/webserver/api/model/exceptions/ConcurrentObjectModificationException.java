package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class ConcurrentObjectModificationException extends RuntimeException implements CasperHttpException {
    public ConcurrentObjectModificationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.CONCURRENT_OBJECT_UPDATE;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.CONCURRENT_OBJECT_UPDATE;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
