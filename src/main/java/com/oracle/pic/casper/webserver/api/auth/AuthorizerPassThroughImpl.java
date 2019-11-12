package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.objectmeta.NamespaceKey;
import com.oracle.pic.casper.webserver.api.model.exceptions.NamespaceDeletedException;
import com.oracle.pic.casper.webserver.api.ratelimit.Embargo;
import com.oracle.pic.casper.webserver.limit.ResourceLimitHelper;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.limit.ResourceType;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.commons.metadata.entities.Compartment;
import com.oracle.pic.tagging.client.tag.TagSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

public class AuthorizerPassThroughImpl implements Authorizer {
    private static final Logger LOG = LoggerFactory.getLogger(AuthorizerPassThroughImpl.class);

    private final Embargo embargo;
    private final ResourceLimiter resourceLimiter;

    public AuthorizerPassThroughImpl(Embargo embargo, ResourceLimiter resourceLimiter) {
        this.embargo = embargo;
        this.resourceLimiter = resourceLimiter;
    }

    @Override
    public Optional<AuthorizationResponse> authorize(WSRequestContext context,
                                                    AuthenticationInfo authInfo,
                                                    NamespaceKey namespaceKey,
                                                    String bucketName,
                                                    String compartmentId,
                                                    BucketPublicAccessType bucketAccessType,
                                                    CasperOperation operation,
                                                    String kmsKeyId,
                                                    boolean authorizeAll,
                                                    boolean tenancyDeleted,
                                                    CasperPermission... permissions) {
        return doAuthorization(context, authInfo, compartmentId, operation, null, tenancyDeleted, permissions);
    }

    @Override
    public Optional<AuthorizationResponse> authorizeWithTags(WSRequestContext context,
                                                   AuthenticationInfo authInfo,
                                                   @Nullable NamespaceKey namespaceKey,
                                                   @Nullable String bucketName,
                                                   String compartmentId,
                                                   @Nullable BucketPublicAccessType bucketAccessType,
                                                   CasperOperation operation,
                                                   @Nullable TaggingOperation taggingOperation,
                                                   @Nullable TagSet newTagSet,
                                                   @Nullable TagSet oldTagSet,
                                                   @Nullable KmsKeyUpdateAuth kmsKeyUpdateAuth,
                                                   boolean authorizeAll,
                                                   boolean tenancyDeleted,
                                                   CasperPermission... permissions) {
        return doAuthorization(context, authInfo, compartmentId, operation, newTagSet, tenancyDeleted, permissions);
    }

    private Optional<AuthorizationResponse> doAuthorization(WSRequestContext context,
                                                  AuthenticationInfo authInfo,
                                                  String compartmentId,
                                                  CasperOperation operation,
                                                  TagSet newTagSet,
                                                  boolean tenancyDeleted,
                                                  CasperPermission... permissions) {
        final CommonRequestContext commonContext = context.getCommonRequestContext();

        context.setCompartmentID(compartmentId);
        context.setCompartment(new Compartment());
        commonContext.getMetricScope()
                .annotate("compartmentId", compartmentId)
                .annotate("operation", operation.getSummary());

        //Check our embargos first to avoid hitting authorization service
        context.getVisa().ifPresent(v -> embargo.customs(commonContext, v, authInfo, operation));

        //Set the tenantOcid in the context for the purpose of logging
        context.setRequestorTenantOcid(authInfo.getMainPrincipal().getTenantId());

        ResourceLimitHelper.withResourceControl(resourceLimiter,
                ResourceType.IDENTITY, context.getNamespaceName().orElse(ResourceLimiter.UNKNOWN_NAMESPACE),
                () -> {
            return null;
        });

        LOG.debug("Authorization request for user '{}' to compartment '{}' is authorized by pass-through",
                authInfo.getMainPrincipal().getSubjectId(), compartmentId);
        if (tenancyDeleted) {
            throw new NamespaceDeletedException("This Tenancy is Deleted");
        }

        context.setRequestorTenantOcid(authInfo.getMainPrincipal().getTenantId());

        // For the set of permissions allowed, just return all of the permissions that were passed in.
        return Optional.of(new AuthorizationResponse(null, newTagSet, true, null,
                new HashSet<>(Arrays.asList(permissions)), null));
    }
}
