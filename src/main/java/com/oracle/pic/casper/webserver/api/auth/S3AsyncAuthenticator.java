package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.common.vertx.VertxUtil;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.CompletableFuture;

public class S3AsyncAuthenticator {

    private final S3Authenticator authenticator;

    public S3AsyncAuthenticator(S3Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public CompletableFuture<AuthenticationInfo> authenticate(RoutingContext context, String bodySha256) {
        final CommonRequestContext commonContext = WSRequestContext.getCommonRequestContext(context);
        return VertxUtil.runAsync(commonContext.getMetricScope().child("authenticate"),
            () -> authenticator.authenticate(context, bodySha256));
    }
}
