package com.oracle.pic.casper.webserver.traffic;

import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * Casper HTTP Exception that represents throttling failures and return the right error code(429) to the client
 */
public class TenantThrottleException extends RuntimeException implements CasperHttpException {

    public TenantThrottleException(String message) {
        super(message);
    }

    @Override
    public ErrorCode errorCode() {
        return V2ErrorCode.TOO_MANY_REQUESTS;
    }

    @Override
    public ErrorCode s3ErrorCode() {
        return S3ErrorCode.TOO_MANY_REQUESTS;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
