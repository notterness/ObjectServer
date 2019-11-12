package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class BucketReplicationEnabledException extends RuntimeException implements CasperHttpException {
    public BucketReplicationEnabledException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.BUCKET_REPLICATION_ENABLED;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.BUCKET_IS_READONLY;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
