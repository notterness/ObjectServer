package com.webutils.webserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for the Athena web server.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // if no '-Dregion=XXX' is provided then we assume that it's a LOCAL one.
        LOG.info("log4j WebServer initialized.");
        System.out.println("console WebServer initialized.");
    }
}
