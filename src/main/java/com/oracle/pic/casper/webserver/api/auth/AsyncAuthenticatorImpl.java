package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

public class AsyncAuthenticatorImpl implements AsyncAuthenticator {
    private final Authenticator authenticator;

    public AsyncAuthenticatorImpl(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public CompletableFuture<AuthenticationInfo> authenticate(RoutingContext context) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        return VertxUtil.runAsync(commonContext.getMetricScope().child("authenticate"),
                () -> authenticator.authenticate(context));
    }

    public CompletableFuture<AuthenticationInfo> authenticatePutObject(RoutingContext context) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        return VertxUtil.runAsync(commonContext.getMetricScope().child("authenticate"),
                () -> authenticator.authenticatePutObject(context));
    }

    public AuthenticationInfo authenticatePutObject(javax.ws.rs.core.HttpHeaders headersOrig, String uriString, String namespace, String method, MetricScope metricScope) {
        return authenticator.authenticatePutObject(headersOrig, uriString, namespace, method, metricScope);
    }

    public CompletableFuture<AuthenticationInfo> authenticate(RoutingContext context, String bodySha256) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        return VertxUtil.runAsync(commonContext.getMetricScope().child("authenticate"),
                () -> authenticator.authenticate(context, bodySha256));
    }
}
