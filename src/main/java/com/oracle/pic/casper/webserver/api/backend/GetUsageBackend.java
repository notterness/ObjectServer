package com.oracle.pic.casper.webserver.api.backend;

import com.oracle.bmc.objectstorage.model.UsageSummary;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.AuthorizationException;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.AuthorizationResponse;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.model.exceptions.NoSuchNamespaceException;
import com.oracle.pic.casper.webserver.api.usage.UsageCache;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GetUsageBackend {
    private final UsageCache usageCache;
    private final Authorizer authorizer;
    private final ResourceControlledMetadataClient metadataClient;

    public GetUsageBackend(
        UsageCache usageCache, Authorizer authorizer, ResourceControlledMetadataClient metadataClient) {
        this.usageCache = usageCache;
        this.authorizer = authorizer;
        this.metadataClient = metadataClient;
    }

    public List<UsageSummary> getStorageUsage(
        RoutingContext context, AuthenticationInfo authInfo, String tenantOcid) {

        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        Optional<AuthorizationResponse> responseOptional = authorizer.authorize(
            wsRequestContext,
            authInfo,
            null,
            null,
            tenantOcid,
            null,
            CasperOperation.GET_USAGE,
            null,
            false,
            false,
            CasperPermission.INTERNAL_CP_USAGE_READ);
        //not empty if the authorization succeeded, empty otherwise.
        if (!responseOptional.isPresent()) {
            throw new AuthorizationException(
                V2ErrorCode.FORBIDDEN.getStatusCode(),
                V2ErrorCode.FORBIDDEN.getErrorName(),
                "Not authorized to getUsage");
        }

        // Get namespace given tenant ocid
        final Optional<Tenant> tenant =
            metadataClient.getTenantByCompartmentId(tenantOcid, ResourceLimiter.UNKNOWN_NAMESPACE);
        if (!tenant.isPresent()) {
            //This should never occur if the user has authenticated.
            throw new NoSuchNamespaceException("Either the namespace" +
                " does not exist or you are not authorized to view it");
        }
        final String namespace = tenant.get().getNamespace();

        final Map<String, Long> map = usageCache.getUsageFromCache(namespace);

        final List<UsageSummary> list = new ArrayList<>();

        // Return the non-aggregated usage :
        // A list of ALL the compartmentIds that have a non-zero usage for the given limit
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            if (entry.getKey().equals(namespace) || entry.getValue() <= 0L) {
                continue;
            }
            list.add(UsageSummary.builder().compartmentId(entry.getKey()).usage(entry.getValue()).build());
        }
        return list;
    }
}
