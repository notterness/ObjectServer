package com.oracle.pic.casper.webserver;

import com.oracle.pic.casper.common.config.ConfigAvailabilityDomain;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.util.GlobalSystemPropertiesConfigurer;
import com.oracle.pic.casper.webserver.server.WebServer;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for the Casper web server.
 *
 * A web server started from here uses the STANDARD flavor (see WebServerFlavors) and expects to connect to external
 * dependencies like identity, databases, volume-service, storage servers, etc (instead of using in-memory versions).
 *
 * The dependencies are found via configuration files which are specified using a stage, region and AD. These can be
 * set using Java properties, for example "-Dregion=us-ashburn-1 -Dstage=prod -Dad=ad1" on the command line.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private Main() {

    }

    public static void main(String[] args) {
        GlobalSystemPropertiesConfigurer.configure();

        final ConfigRegion region = ConfigRegion.fromSystemProperty();
        final ConfigAvailabilityDomain ad = ConfigAvailabilityDomain.tryFromSystemProperty()
                .orElse(ConfigAvailabilityDomain.GLOBAL);

        try {
            final CasperConfig config = CasperConfig.create(region, ad);
            final WebServer webServer = new WebServer(WebServerFlavor.INTEGRATION_TESTS, config);
            webServer.start();
        } catch (Exception ex) {
            LOG.error("The web server encountered an exception during initialization, shutting down", ex);
            System.exit(1);
        }
    }
}
