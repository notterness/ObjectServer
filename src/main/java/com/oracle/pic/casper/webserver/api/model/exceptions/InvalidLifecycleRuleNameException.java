package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class InvalidLifecycleRuleNameException extends RuntimeException implements CasperHttpException {
    public InvalidLifecycleRuleNameException(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.INVALID_RULE_NAME;
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
