package com.oracle.pic.casper.webserver.api.auth;

import io.vertx.ext.web.RoutingContext;

//uses SIGv4 for authentication
public interface S3Authenticator {
    //authenticate requests with body, throws NotAuthenticatedException if not successful
    AuthenticationInfo authenticate(RoutingContext context, String bodySha256);
}
