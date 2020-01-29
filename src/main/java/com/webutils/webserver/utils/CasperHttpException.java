package com.webutils.webserver.utils;


public interface CasperHttpException {
    ErrorCode errorCode();

    ErrorCode s3ErrorCode();

    String errorMessage();
}
