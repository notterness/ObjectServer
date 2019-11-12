package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class InvalidMetadataException extends RuntimeException implements CasperHttpException {
    public InvalidMetadataException(String msg) {
        super(msg);
    }

    public InvalidMetadataException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.METADATA_TOO_LARGE;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.METADATA_TOO_LARGE;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
