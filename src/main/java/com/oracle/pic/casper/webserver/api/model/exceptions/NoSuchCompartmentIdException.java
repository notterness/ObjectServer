package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

import java.sql.SQLException;

/**
 * An exception thrown when the operation is called with the compartmentID that cannot be found.
 */
public class NoSuchCompartmentIdException extends RuntimeException implements CasperHttpException {

    /**
     * Instantiates a new {@code NoSuchNamespaceException}.
     *
     * @param message the message
     */
    public NoSuchCompartmentIdException(String message) {
        super(message);
    }

    /**
     * Instantiates a new {@code NoSuchNamespaceException}.
     *
     * @param message the message
     * @param cause   the cause
     */
    public NoSuchCompartmentIdException(String message, SQLException cause) {
        super(message, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.COMPARTMENT_ID_NOT_FOUND;
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
