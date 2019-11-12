package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.NotAuthenticatedException;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.model.exceptions.RelatedResourceNotAuthorizedOrNotFound;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Common validators for V2, Swift and S3 bucket creations
 */
public final class V2BucketCreationChecks {

    private V2BucketCreationChecks() {
    }

    /**
     * Validates the namespace specified in the URL is associated with the calling user's tenancy or specified the
     * compartment id.
     * @param metadataClient
     * @param authInfo
     * @param compartmentId is nullable for S3 calls but must be specified for V2 calls. This check is done in the
     *                      calling function.
     * @param namespace
     * @return the Tenant associated with the namespace.
     */
    public static Tenant checkNamespace(
        ResourceControlledMetadataClient metadataClient,
        AuthenticationInfo authInfo,
        @Nullable String compartmentId,
        String namespace) {

        if (authInfo == AuthenticationInfo.ANONYMOUS_USER) {
            throw new NotAuthenticatedException("Unauthenticated requests are not allowed");
        }

        // check that the given namespace name matches the user's tenant name. In the future we may stop
        // using tenant name as namespace name (and require users to create a namespace name via an API).
        final Optional<Tenant> tenant = metadataClient.getTenantByCompartmentId(
                authInfo.getMainPrincipal().getTenantId(), namespace
        );

        // this may be a cross-tenant bucket create request, thus the given namespace name must match the tenant name
        // of the compartment specified in the request.
        if (compartmentId != null) {
            Optional<Tenant> tenantFromCompartmentId =
                    metadataClient.getTenantByCompartmentId(compartmentId, namespace);
            if (tenantFromCompartmentId.isPresent() &&
                    tenantFromCompartmentId.get().getNamespace().equals(namespace)) {
                return tenantFromCompartmentId.get();
            }
        } else {
            // if compartmentId is not specified, check the that the principal making the request has the same
            // namespace as what is specified in the URI
            if (tenant.isPresent() && tenant.get().getNamespace().equals(namespace)) {
                return tenant.get();
            }
        }

        final String accountNamespace = tenant.map(Tenant::getNamespace).orElse(namespace);

        throw new RelatedResourceNotAuthorizedOrNotFound("The namespace in the URL ('" + namespace +
                                            "') must match the namespace of the account ('" + accountNamespace +
                                            "') or of the compartment in the request ('" + compartmentId + "')");
    }
}
