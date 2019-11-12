package com.oracle.pic.casper.webserver.api.model.serde;

/**
 * Exception which indicates that a bucket update JSON string was invalid (bad JSON format or bad contents).
 */
public class InvalidBucketJsonException extends RuntimeException {
    public InvalidBucketJsonException(String msg) {
        super(msg);
    }
}
