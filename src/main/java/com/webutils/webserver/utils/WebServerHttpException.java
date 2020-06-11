package com.webutils.webserver.utils;


public interface WebServerHttpException {
    ErrorCode errorCode();

    ErrorCode s3ErrorCode();

    String errorMessage();
}
