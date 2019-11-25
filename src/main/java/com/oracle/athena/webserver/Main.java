package com.oracle.athena.webserver;

import com.oracle.athena.webserver.server.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for the Athena web server.
 * <p>
 * TODO At the moment this is a very simple web server that is not affected by config or logging.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // TODO: Determine how many threads our webserver should actually consume when deployed
        WebServer server = new WebServer(1);
        server.start();
        LOG.info("Athena WebServer initialized.");
    }
}
