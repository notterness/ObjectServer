package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class NoSuchNamespaceException extends RuntimeException implements CasperHttpException {
    public NoSuchNamespaceException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.NAMESPACE_NOT_FOUND;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.NO_SUCH_BUCKET;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
