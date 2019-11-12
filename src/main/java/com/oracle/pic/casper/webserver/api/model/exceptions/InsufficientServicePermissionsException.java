package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class InsufficientServicePermissionsException extends RuntimeException implements CasperHttpException {

    public InsufficientServicePermissionsException(String message) {
        super(message);
    }

    @Override
    public ErrorCode errorCode() {
        return V2ErrorCode.INSUFFICIENT_SERVICE_PERMISSIONS;
    }

    @Override
    public ErrorCode s3ErrorCode() {
        // TODO(jfriedly):  When we implement S3 support for OLM, this will need an entry in S3ErrorCode.
        return null;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
