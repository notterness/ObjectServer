package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.exceptions.NotFoundException;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * A handler meant to be used as the last handler in a router chain, that throws an HttpException if no routes
 * matched.
 *
 * We use custom FailureHandler logic to turn HttpExceptions into HTTP responses, so this works for Swift, Casper v2
 * (and any future APIs).
 */
public class NotFoundHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext context) {
        throw new NotFoundException("Not Found");
    }
}
