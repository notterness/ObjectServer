package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class NoSuchTagSetException extends RuntimeException implements CasperHttpException {
    public NoSuchTagSetException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return null;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.NO_SUCH_TAGSET;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
