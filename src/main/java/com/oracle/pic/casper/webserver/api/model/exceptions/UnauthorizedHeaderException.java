package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class UnauthorizedHeaderException extends RuntimeException implements CasperHttpException {

    public UnauthorizedHeaderException() {
        super("Unauthorized use of header");
    }

    @Override
    public ErrorCode errorCode() {
        return V2ErrorCode.UNAUTHORIZED;
    }

    @Override
    public ErrorCode s3ErrorCode() {
        return null;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
