package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.webserver.auth.AuthTestConstants;
import com.oracle.pic.casper.webserver.limit.ResourceLimitHelper;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.limit.ResourceType;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.commons.metadata.entities.User;
import com.oracle.pic.identity.authentication.PrincipalImpl;
import com.oracle.pic.identity.authorization.sdk.AuthorizationRequest;
import io.vertx.ext.web.RoutingContext;

import javax.annotation.Nullable;

/**
 * An {@link Authenticator} that accepts any request as authenticated.
 */
public class AuthenticatorPassThroughImpl implements Authenticator {

    private String tenantId;
    private String subjectId;
    private final ResourceLimiter resourceLimiter;

    public AuthenticatorPassThroughImpl(ResourceLimiter resourceLimiter) {
        this.tenantId = AuthTestConstants.FAKE_TENANT_ID;
        this.subjectId = AuthTestConstants.FAKE_SUBJECT_ID;
        this.resourceLimiter = resourceLimiter;
    }

    @Override
    public AuthenticationInfo authenticate(RoutingContext context) {
        return authenticatePassThrough(context);
    }

    @Override
    public AuthenticationInfo authenticate(RoutingContext context, String bodySha256) {
        return authenticatePassThrough(context);
    }

    @Override
    public AuthenticationInfo authenticatePutObject(RoutingContext context) {
        return authenticatePassThrough(context);
    }

    @Override
    public AuthenticationInfo authenticateSwiftOrCavage(RoutingContext context,
                                                        @Nullable String bodySha256,
                                                        String namespace,
                                                        @Nullable String tenantOcid) {
        return authenticatePassThrough(context);
    }

    @Override
    public AuthenticationInfo authenticateSwiftOrCavageForPutObject(RoutingContext context,
                                                                    String namespace,
                                                                    @Nullable String tenantOcid) {
        return authenticatePassThrough(context);
    }

    @Override
    public AuthenticationInfo authenticateCavageOrJWT(RoutingContext context, @Nullable String bodySha256) {
        return authenticatePassThrough(context);
    }

    public void setTenant(String tenantId, String subjectId) {
        this.tenantId = tenantId;
        this.subjectId = subjectId;
    }

    private AuthenticationInfo authenticatePassThrough(RoutingContext context) {
        WSRequestContext.getMetricScope(context)
            .annotate("tenantId", tenantId)
            .annotate("subjectId", subjectId);

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        wsRequestContext.setPrincipal(new PrincipalImpl(tenantId, subjectId));
        wsRequestContext.setUser(new User());

        ResourceLimitHelper.withResourceControl(resourceLimiter,
                ResourceType.IDENTITY, wsRequestContext.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE),
                () -> {
            return null;
        });

        if (context.request().getHeader("obo") != null) {
            return new AuthenticationInfo(new PrincipalImpl(tenantId, subjectId),
                    new PrincipalImpl(tenantId, subjectId),  AuthorizationRequest.Kind.OBO);
        } else {
            return AuthenticationInfo.fromPrincipal(new PrincipalImpl(tenantId, subjectId));
        }
    }
}
