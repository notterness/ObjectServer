package com.oracle.pic.casper.webserver.api.common;


import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

/**
 * This class handles CORS request for OPTION
 */
public class OptionCorsHandler implements Handler<RoutingContext> {

    public OptionCorsHandler() {

    }

    @Override
    public void handle(RoutingContext context) {

        final HttpServerResponse response = context.response();

        response.setStatusCode(HttpResponseStatus.OK);
        response.putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, "POST,PUT,GET,HEAD,DELETE,OPTIONS");
        response.putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");

        // Mirror requested ones
        String requestedHeaders = context.request().getHeader(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS);
        if (requestedHeaders != null) {
            response.putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, requestedHeaders);
        }

        response.end();
    }
}
