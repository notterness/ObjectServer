package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class InvalidTagKeyException extends RuntimeException implements CasperHttpException {

    public InvalidTagKeyException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.INVALID_TAG_KEY;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.INVALID_TAG;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
