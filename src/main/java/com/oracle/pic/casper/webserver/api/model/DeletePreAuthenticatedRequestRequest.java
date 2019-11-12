package com.oracle.pic.casper.webserver.api.model;

import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.pars.BackendParId;
import io.vertx.ext.web.RoutingContext;

public final class DeletePreAuthenticatedRequestRequest {

    // the routing context of the request
    private final RoutingContext context;

    // the authentication info to send to Identity
    private final AuthenticationInfo authInfo;

    // the unique identifier to the PAR
    private final BackendParId parId;

    public DeletePreAuthenticatedRequestRequest(RoutingContext context, AuthenticationInfo authInfo,
                                                BackendParId parId) {
        this.context = context;
        this.authInfo = authInfo;
        this.parId = parId;

    }

    public RoutingContext getContext() {
        return context;
    }

    public AuthenticationInfo getAuthInfo() {
        return authInfo;
    }

    public BackendParId getParId() {
        return parId;
    }

    @Override
    public String toString() {
        return "DeletePreAuthenticatedRequestRequest{" +
                "authInfo=" + authInfo +
                ", parId=" + parId +
                '}';
    }
}
