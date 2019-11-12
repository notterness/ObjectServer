package com.oracle.pic.casper.webserver.server;

/**
 * A web server "flavor" that controls the down stream dependencies used by the web server.
 */
public enum WebServerFlavor {

    /**
     * The downstream dependencies are real, external services with configured endpoints. This flavor is used by
     * production web servers and our system and smoke tests.
     */
    STANDARD,

    /**
     * The downstream dependencies are all in-memory, fake implementations, and no external services are used. This
     * flavor is used by web server integration tests.
     */
    INTEGRATION_TESTS,
}
