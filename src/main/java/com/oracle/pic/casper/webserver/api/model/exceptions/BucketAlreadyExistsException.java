package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class BucketAlreadyExistsException extends RuntimeException implements CasperHttpException {
    public BucketAlreadyExistsException(String msg) {
        super(msg);
    }

    public BucketAlreadyExistsException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.BUCKET_ALREADY_EXISTS;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.BUCKET_ALREADY_EXISTS;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
