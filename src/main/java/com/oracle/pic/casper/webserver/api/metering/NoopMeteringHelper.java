package com.oracle.pic.casper.webserver.api.metering;

import io.vertx.ext.web.RoutingContext;

/**
 * A no-operation {@link MeteringHelper} that is used at the moment for requests
 * that won't be using any sort of metering and will be phased out
 * (i.e. {@link com.oracle.pic.casper.webserver.api.v2.GetNamespaceCollectionHandler}
 */
public class NoopMeteringHelper implements MeteringHelper {

    @Override
    public void logMeteredRequest(RoutingContext context) {
        //no-op
    }

    @Override
    public void logMeteredBandwidth(RoutingContext context) {
        //no-op
    }
}
