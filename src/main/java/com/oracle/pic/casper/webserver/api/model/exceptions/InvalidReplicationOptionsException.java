package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class InvalidReplicationOptionsException extends RuntimeException implements CasperHttpException {
    public InvalidReplicationOptionsException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.INVALID_REPLICATION_OPTIONS;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.INVALID_REQUEST;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
