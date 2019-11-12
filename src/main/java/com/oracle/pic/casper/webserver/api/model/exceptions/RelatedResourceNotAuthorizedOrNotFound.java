package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

public class RelatedResourceNotAuthorizedOrNotFound extends RuntimeException implements CasperHttpException {

    public RelatedResourceNotAuthorizedOrNotFound(String msg) {
        super(msg);
    }

    @Override
    public V2ErrorCode errorCode() {
        return V2ErrorCode.RELATED_RESOURCE_NOT_FOUND;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.SIGNATURE_DOES_NOT_MATCH;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
