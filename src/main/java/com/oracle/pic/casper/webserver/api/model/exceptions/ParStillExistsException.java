package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * Thrown when trying to delete a bucket for which PARs still exist
 */
public class ParStillExistsException extends RuntimeException implements CasperHttpException {
    public ParStillExistsException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.PAR_STILL_EXISTS;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.PAR_STILL_EXISTS;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
