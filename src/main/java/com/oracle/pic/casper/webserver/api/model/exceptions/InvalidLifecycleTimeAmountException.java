package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class InvalidLifecycleTimeAmountException extends RuntimeException implements CasperHttpException {
    public InvalidLifecycleTimeAmountException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.INVALID_TIME_AMOUNT;
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
