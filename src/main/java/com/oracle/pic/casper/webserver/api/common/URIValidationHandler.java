package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.webserver.api.model.exceptions.MalformedURIException;
import io.vertx.core.Handler;
import io.vertx.core.http.impl.HttpUtils;
import io.vertx.core.net.impl.URIDecoder;
import io.vertx.ext.web.RoutingContext;

/**
 * This handler is written to handle the tricky exception handling when a malformed URI (e.g. those with unterminated
 * % encoding) reaches the web server and Vert.x tries to decode it, do regex matching, and dispatch to the appropriate
 * request handlers. When Vert.x tries to decode the malformed URI, it throws IllegalArgumentException, which we don't
 * have any control over. We can't just catch all IllegalArgumentExceptions either, because that will mask potential
 * 500s. Catching it based on error message strings, etc. are too hacky and unreliable.
 *
 * The solution is to add this handler, which proactively calls Vert.x's
 *
 * {@link io.vertx.core.net.impl.URIDecoder#decodeURIComponent(String, boolean)}
 * {@link io.vertx.core.http.impl.HttpUtils#normalizePath(String)}
 *
 * methods and handles its exceptions properly, before Vert.x actually tries to decode the URI to decide which request
 * handlers to dispatch to. This is also hacky, but hopefully more reliable - there isn't really a better way ATM.
 *
 * This handler should be added to every API before any request handlers that matches URI by regex.
 */
public class URIValidationHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext context) {
        try {
            final String path = context.request().path();
            URIDecoder.decodeURIComponent(path, false);
            HttpUtils.normalizePath(path);
        } catch (IllegalArgumentException iae) {
            throw new MalformedURIException(iae.getMessage());
        }
        context.next();
    }
}
