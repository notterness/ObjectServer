package com.oracle.athena.webserver;

import com.oracle.athena.webserver.server.ServerChannelLayer;
import com.oracle.athena.webserver.server.WebServer;

/**
 * The entry point for the Athena web server.
 * <p>
 * TODO At the moment this is a very simple web server that is not affected by config or logging.
 */
public class Main {

    public static void main(String[] args) {
        System.out.println("ServerTest serverConnId: " + ServerChannelLayer.DEFAULT_CLIENT_ID);
        // TODO: Determine how many threads our webserver should actually consume when deployed
        WebServer server = new WebServer(1);
        server.start();
    }
}
