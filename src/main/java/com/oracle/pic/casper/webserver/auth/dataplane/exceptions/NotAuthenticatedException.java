package com.oracle.pic.casper.webserver.auth.dataplane.exceptions;

public class NotAuthenticatedException extends RuntimeException {

    public NotAuthenticatedException() {
        this("The required information to complete authentication was not provided or was incorrect.");
    }

    public NotAuthenticatedException(String message) {
        super(message);
    }
}
