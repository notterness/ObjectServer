package com.oracle.pic.casper.webserver.traffic;

import com.oracle.pic.casper.common.error.ErrorCode;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.CasperHttpException;

/**
 * TrafficControlException is thrown to indicate that a web server request has been rejected by the TrafficController.
 *
 * See the class docs for the TrafficController for information about when and why traffic can be rejected.
 */
public class TrafficControlException extends RuntimeException implements CasperHttpException {
    public TrafficControlException(String msg) {
        super(msg);
    }

    @Override
    public ErrorCode errorCode() {
        return V2ErrorCode.SERVICE_UNAVAILABLE;
    }

    @Override
    public S3ErrorCode s3ErrorCode() {
        return S3ErrorCode.SERVICE_UNAVAILABLE;
    }

    @Override
    public String errorMessage() {
        return getMessage();
    }
}
