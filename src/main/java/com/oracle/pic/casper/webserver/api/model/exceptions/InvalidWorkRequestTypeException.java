package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * An exception thrown when an attempt was made to list work request with an invalid work request type
 */
public class InvalidWorkRequestTypeException extends RuntimeException implements CasperHttpException {
    /**
     * Instantiates a new {@code InvalidCompartmentIdException}.
     *
     * @param message the message
     */
    public InvalidWorkRequestTypeException(String message) {
        super(message);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.INVALID_WORK_REQUEST_TYPE;
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
