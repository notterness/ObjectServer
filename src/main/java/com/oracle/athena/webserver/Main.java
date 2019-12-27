package com.oracle.athena.webserver;

import com.oracle.athena.webserver.common.GlobalSystemPropertiesConfigurator;
//import com.oracle.athena.webserver.server.WebServer;
import com.oracle.pic.casper.common.config.ConfigAvailabilityDomain;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.webserver.server.WebServerFlavor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for the Athena web server.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // Configure loggers before anything else - this may be unnecessary once we're clear of Vert.x
        GlobalSystemPropertiesConfigurator.configure();
        // if no '-Dregion=XXX' is provided then we assume that it's a LOCAL one.
        ConfigRegion region = ConfigRegion.tryFromSystemProperty().orElse(ConfigRegion.LOCAL);
        final ConfigAvailabilityDomain ad =
                ConfigAvailabilityDomain.tryFromSystemProperty().orElse(ConfigAvailabilityDomain.GLOBAL);
        //WebServer server = new WebServer(WebServerFlavor.INTEGRATION_TESTS, CasperConfig.create(region, ad));
        //server.start();
        LOG.info("Athena WebServer initialized.");
    }
}
