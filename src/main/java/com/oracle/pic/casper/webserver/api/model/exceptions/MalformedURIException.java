package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class MalformedURIException extends RuntimeException implements CasperHttpException {
    public MalformedURIException(String msg) {
        super(msg);
    }

    @Override
    public ErrorCode errorCode() {
        return V2ErrorCode.MALFORMED_URI;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.MALFORMED_URI;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
