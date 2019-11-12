package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.webserver.auth.AuthTestConstants;
import com.oracle.pic.casper.webserver.api.auth.sigv4.SIGV4Authorization;
import com.oracle.pic.casper.webserver.api.auth.sigv4.SIGV4Utils;
import com.oracle.pic.casper.webserver.limit.ResourceLimitHelper;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.limit.ResourceType;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.commons.metadata.entities.User;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalImpl;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.time.Instant;

public class S3AuthenticatorPassThroughImpl implements S3Authenticator {


    private String tenantId;
    private String subjectId;
    private final ResourceLimiter resourceLimiter;

    public S3AuthenticatorPassThroughImpl(ResourceLimiter resourceLimiter) {
        this.tenantId = AuthTestConstants.FAKE_TENANT_ID;
        this.subjectId = AuthTestConstants.FAKE_SUBJECT_ID;
        this.resourceLimiter = resourceLimiter;
    }

    public void setTenant(String tenantId, String subjectId) {
        this.tenantId = tenantId;
        this.subjectId = subjectId;
    }

    @Override
    public AuthenticationInfo authenticate(RoutingContext context, String bodySha256) {
        HttpServerRequest request = context.request();
        if (request.query() != null &&
                request.query().contains(SIGV4Authorization.X_AMZ_ALGORITHM)) {
            Instant expireTime = SIGV4Utils.getExpireTime(request.getParam(SIGV4Utils.X_AMZ_DATE),
                    request.getParam(SIGV4Authorization.X_AMZ_EXPIRES));
            if (expireTime.isBefore(Instant.now())) {
                throw new NotAuthenticatedException("Invalid authentication information");
            }
        }
        final Principal principal = new PrincipalImpl(tenantId, subjectId);
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setPrincipal(principal);
        wsRequestContext.setUser(new User());

        ResourceLimitHelper.withResourceControl(resourceLimiter,
                ResourceType.IDENTITY, wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE),
                () -> {
            return null;
        });

        return AuthenticationInfo.fromPrincipal(principal);
    }
}
