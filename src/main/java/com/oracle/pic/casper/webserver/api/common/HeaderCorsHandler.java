package com.oracle.pic.casper.webserver.api.common;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

public class HeaderCorsHandler implements Handler<RoutingContext> {
    @Override
    public void handle(RoutingContext context) {
        context.addHeadersEndHandler(v -> CorsHelpers.addCorsHeaders(context.request(), context.response()));
        context.next();
    }
}
