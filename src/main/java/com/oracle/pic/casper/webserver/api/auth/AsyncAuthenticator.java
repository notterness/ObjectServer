package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.metrics.MetricScope;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

public interface AsyncAuthenticator {
    CompletableFuture<AuthenticationInfo> authenticate(RoutingContext context);
    CompletableFuture<AuthenticationInfo> authenticate(RoutingContext context, String bodySha256);
    CompletableFuture<AuthenticationInfo> authenticatePutObject(RoutingContext context);
    AuthenticationInfo authenticatePutObject(javax.ws.rs.core.HttpHeaders headersOrig, String uriString, String namespace, String method, MetricScope metricScope);
}
