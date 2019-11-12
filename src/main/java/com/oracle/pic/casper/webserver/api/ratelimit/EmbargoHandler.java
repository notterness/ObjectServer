package com.oracle.pic.casper.webserver.api.ratelimit;

import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;

@Deprecated
public class EmbargoHandler implements Handler<RoutingContext> {

    /**
     * Clients are not necessarily required to supply a user-agent when they perform requests.
     * The way embargo works however is *null* is a match all value.
     * In order to have the ability to restrict those without user-agents we actually supply them a default one
     * rather than null.
     */
    private static final String DEFAULT_USER_AGENT = "DEFAULT_USER_AGENT";

    private final Embargo embargo;

    public EmbargoHandler(Embargo embargo) {
        this.embargo = embargo;
    }

    @Override
    public void handle(RoutingContext context) {
        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final CommonRequestContext commonRequestContext = wsRequestContext.getCommonRequestContext();
        final String userAgent = Optional.ofNullable(request.getHeader(HttpHeaders.USER_AGENT))
                .orElse(DEFAULT_USER_AGENT);
        //Check that the request hasn't been embargoed.
        final EmbargoContext visa = embargo.enter(commonRequestContext, request.method().name(), request.uri(),
                userAgent);
        //Set the visa on the WSRequestContext so we can look it up in the failure handler and authenticator
        wsRequestContext.setVisa(visa);
        //Register leaving casper
        wsRequestContext.pushEndHandler(c -> embargo.exit(visa));
        context.next();
    }

}
