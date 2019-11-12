package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperException;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;
import com.oracle.pic.casper.common.exceptions.ExceptionHttpStatus;

@ExceptionHttpStatus(value = 404, statusMessage = "PreAuthenticatedRequestNotFound")
public class ParNotFoundException extends CasperException implements CasperHttpException {

    public ParNotFoundException() {
        super();
    }

    public ParNotFoundException(Throwable cause) {
        super(cause);
    }

    public ParNotFoundException(String message) {
        super(message);
    }

    public ParNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean stackTrace) {
        super(message, cause, enableSuppression, stackTrace);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.PAR_NOT_FOUND;
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
