package com.oracle.athena.webserver.server;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ServerChannelLayerTest {

    private static ServerChannelLayer server;

    /**
     * Before any test in this class is run, start up a server.
     */
    @BeforeAll
    private static void beforeAllTests() {
        /*
            FIXME: The number of threads, port selected, and client ID provided should be dynamic
            Right now, these values are either defaulted or using a fixed value, but as we scale this test suite to be a
            full fledged test suite these values will eventually collide.  To remedy this, we should have some global
            state keeping track of what ports are being used by which tests and which clients are assigned to them.

            Load tests should be handled in a separate test area similar to how the "manual" ones are handled today.
         */

        server = new ServerChannelLayer(1, 1);
        server.start();
    }

    /**
     * After every test in this class has run, stop the server.
     */
    @AfterAll
    private static void afterAllTests() {
        server.stop();
    }

    /**
     * This test merely attempts to connect a client to our custom Web Server and send a simple message across to it.
     */
    @Test
    public void validateBasicConnection() {
        System.out.println("Hello sunshine!");
    }
}
