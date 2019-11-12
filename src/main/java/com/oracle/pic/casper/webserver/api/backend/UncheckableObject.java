package com.oracle.pic.casper.webserver.api.backend;

import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * Raised when an object cannot be checked by {@link ControlAPIGetObjectBackend#checkObject
 * String, String)} because, for example, it's multipart.
 */
public class UncheckableObject extends RuntimeException implements CasperHttpException {
    UncheckableObject(String message) {
        super(message);
    }

    @Override
    public ErrorCode errorCode() {
        return V2ErrorCode.NOT_IMPLEMENTED;
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
