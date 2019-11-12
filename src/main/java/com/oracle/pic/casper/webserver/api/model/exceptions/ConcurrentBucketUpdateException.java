package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * Exception which indicates that a bucket update was done concurrently and failed to commit.
 */
public class ConcurrentBucketUpdateException extends RuntimeException implements CasperHttpException {
    public ConcurrentBucketUpdateException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.CONCURRENT_BUCKET_UPDATE;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return null;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
