package com.webutils.webserver.utils;

public class WebServerException extends RuntimeException {
    public WebServerException() {
    }

    public WebServerException(Throwable cause) {
        super(cause);
    }

    public WebServerException(String message) {
        super(message);
    }

    public WebServerException(String message, Throwable cause) {
        super(message, cause);
    }

    public WebServerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
