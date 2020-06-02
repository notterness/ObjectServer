package com.webutils.webserver.utils;

public class CasperException extends RuntimeException {
    public CasperException() {
    }

    public CasperException(Throwable cause) {
        super(cause);
    }

    public CasperException(String message) {
        super(message);
    }

    public CasperException(String message, Throwable cause) {
        super(message, cause);
    }

    public CasperException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
