package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * An exception thrown when an attempt was made to fetch namespace of a tenancy by providing a invalid compartmentId or
 * a compartmentId which is not an tenantId.
 */
public class InvalidCompartmentIdException extends RuntimeException implements CasperHttpException {

    /**
     * Instantiates a new {@code InvalidCompartmentIdException}.
     *
     * @param message the message
     */
    public InvalidCompartmentIdException(String message) {
        super(message);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.INVALID_COMPARTMENT_ID;
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
