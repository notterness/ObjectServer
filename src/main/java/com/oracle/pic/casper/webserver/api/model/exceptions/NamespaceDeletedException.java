package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class NamespaceDeletedException extends RuntimeException implements CasperHttpException {

    public NamespaceDeletedException(String msg) {
        super(msg);
    }

    public NamespaceDeletedException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.NAMESPACE_DELETED;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.NAMESPACE_DELETED;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
