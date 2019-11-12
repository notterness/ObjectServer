package com.oracle.pic.casper.webserver.api.common;

/**
 * A simple factory that guice will use to create a {@link FailureHandler}
 */
public interface FailureHandlerFactory {

    FailureHandler create(WSExceptionTranslator wsExceptionTranslator);
}
