package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class BucketReadOnlyException  extends RuntimeException implements CasperHttpException {
    public BucketReadOnlyException(String msg) {
        super(msg);
    }

    public BucketReadOnlyException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.BUCKET_IS_READONLY;
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
