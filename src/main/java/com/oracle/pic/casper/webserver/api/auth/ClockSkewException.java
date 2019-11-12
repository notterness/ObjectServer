package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * ClockSkewException is thrown when the authenticator failed to authenticate the request because the client clock and
 * the server clock are not in sync. This will translate into 400 Bad Request.
 */
public class ClockSkewException extends RuntimeException implements CasperHttpException {
    public ClockSkewException(String message) {
        super(message);
    }

    public ClockSkewException(String message, Exception cause) {
        super(message, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.CLOCK_SKEW;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.REQUEST_TIME_TOO_SKEWED;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
