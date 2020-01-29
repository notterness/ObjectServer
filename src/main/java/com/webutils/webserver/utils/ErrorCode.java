package com.webutils.webserver.utils;

public interface ErrorCode {
    int getStatusCode();

    String getErrorName();

    static ErrorCode generic(final int status, final String name) {
        return new ErrorCode() {
            public int getStatusCode() {
                return status;
            }

            public String getErrorName() {
                return name;
            }
        };
    }
}
