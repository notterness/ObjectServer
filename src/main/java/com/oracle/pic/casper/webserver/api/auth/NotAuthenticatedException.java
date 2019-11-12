package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * NotAuthenticatedException is thrown when the authenticator failed to authenticate the request due to bad
 * credential. This will be translated as 401 Not Authorized in V2 API or 403 Signature Does Not Match on S3 API
 */
public class NotAuthenticatedException extends RuntimeException implements CasperHttpException {
    public NotAuthenticatedException(String message) {
        super(message);
    }

    public NotAuthenticatedException(String message, Exception cause) {
        super(message, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.NOT_AUTHENTICATED;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.SIGNATURE_DOES_NOT_MATCH;
    }

    @Override
    public String errorMessage() {
        return "The required information to complete authentication was not provided.";
    }
}
