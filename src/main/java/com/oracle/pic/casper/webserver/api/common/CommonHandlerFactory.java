package com.oracle.pic.casper.webserver.api.common;

/**
 * A simple interface to help Guice create a {@link CommonHandler}
 * with some of the constructor arguments provided by the user
 */
public interface CommonHandlerFactory {

    /**
     * Constructor.
     * @param useBadRequestException true to use BadRequestException for errors, false to use HttpException.
     * @param casperAPI the Casper API served by this handler.
     */
    CommonHandler create(boolean useBadRequestException, CasperAPI casperAPI);
}
