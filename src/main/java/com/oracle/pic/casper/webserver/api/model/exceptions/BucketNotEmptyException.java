package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * Thrown when trying to delete a non-empty bucket
 */
public class BucketNotEmptyException extends RuntimeException implements CasperHttpException {
    public BucketNotEmptyException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.BUCKET_NOT_EMPTY;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.BUCKET_NOT_EMPTY;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
