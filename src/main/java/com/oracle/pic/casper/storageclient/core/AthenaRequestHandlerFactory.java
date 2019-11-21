package com.oracle.pic.casper.storageclient.core;

/**
 * An interface to construct a request handler based on the volume info.
 */
public interface AthenaRequestHandlerFactory {

    /**
     * Creates a new GET request handler.
     *
     * @return the GET request handler
     */
    GetRequestHandler newGetRequestHandler();

    /**
     * Creates a new PUT request handler.
     *
     * @return the PUT request handler
     */
    AthenaPutRequestHandler newPutRequestHandlerAthena();

    PutRequestHandler newPutRequestHandler();

    /**
     * Creates a new DELETE request handler.
     *
     * @return the DELETE request handler
     */
    DeleteRequestHandler newDeleteRequestHandler();
}

