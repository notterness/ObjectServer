package com.webutils.webserver.common;

import com.google.common.base.MoreObjects;
import com.webutils.webserver.utils.*;

@ExceptionHttpStatus(
        value = 400,
        statusMessage = "InvalidDigest"
)
public class InvalidDigestException extends CasperException implements CasperHttpException {
    private final Digest expected;
    private final Digest actual;

    public InvalidDigestException(String message) {
        super(message);
        this.expected = null;
        this.actual = null;
    }

    public InvalidDigestException(Digest expected, Digest actual) {
        this(expected, actual, "Expected digest differs from actual.");
    }

    public InvalidDigestException(Digest expected, Digest actual, String message) {
        super(message);
        this.expected = expected;
        this.actual = actual;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("message", this.getMessage()).add("expected", this.expected).add("actual", this.actual).toString();
    }

    public V2ErrorCode errorCode() {
        return V2ErrorCode.UNMATCHED_CONTENT_MD5;
    }

    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.BAD_DIGEST;
    }

    public String errorMessage() {
        return this.getMessage();
    }
}
