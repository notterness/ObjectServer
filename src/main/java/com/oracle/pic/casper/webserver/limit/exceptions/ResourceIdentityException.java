package com.oracle.pic.casper.webserver.limit.exceptions;

import com.oracle.pic.casper.common.exceptions.TooBusyException;

public final class ResourceIdentityException extends TooBusyException {

    public ResourceIdentityException() {
        super();
    }

    public ResourceIdentityException(String message) {
        super(message);
    }

    public ResourceIdentityException(String message, Throwable cause) {
        super(message, cause);
    }
}
