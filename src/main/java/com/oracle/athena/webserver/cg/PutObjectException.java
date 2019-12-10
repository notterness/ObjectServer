package com.oracle.pic.casper.webserver.api.backend.putobject;

/**
 * PutObjectException is thrown from PutObjectMachine#start when the PUT object operation failed.
 *
 * The message returned is not meant for customers, so it must not be serialized back to them. The serverConnClean
 * flag indicates whether or not the connection with the client is "clean". The connection is clean if the full HTTP
 * request was read, and it is false if there are more bytes of the request left to read. The caller has the option to
 * close the connection or continue reading bytes until it has been exhausted (the state machine does not make that
 * decision). See the documentation for PutObjectMachine for more details on error handling.
 */
public class PutObjectException extends RuntimeException {
    private final boolean serverConnClean;

    public PutObjectException(String message, Throwable cause, boolean serverConnClean) {
        super(message, cause);

        this.serverConnClean = serverConnClean;
    }

    public boolean isServerConnClean() {
        return serverConnClean;
    }
}
