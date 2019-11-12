package com.oracle.pic.casper.webserver.api.model.exceptions;

import com.oracle.pic.casper.common.exceptions.CasperException;
import com.oracle.pic.casper.common.exceptions.ExceptionHttpStatus;

@ExceptionHttpStatus(value = 409, statusMessage = "PreAuthenticatedRequestAlreadyExists")
public class ParAlreadyExistsException extends CasperException {
    public ParAlreadyExistsException() {
        super();
    }

    public ParAlreadyExistsException(Throwable cause) {
        super(cause);
    }

    public ParAlreadyExistsException(String message) {
        super(message);
    }

    public ParAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParAlreadyExistsException(String message, Throwable cause, boolean enableSuppression, boolean stackTrace) {
        super(message, cause, enableSuppression, stackTrace);
    }
}
