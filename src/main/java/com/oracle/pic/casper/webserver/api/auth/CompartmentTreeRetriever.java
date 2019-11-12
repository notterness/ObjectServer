package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.commons.metadata.entities.CompartmentTree;
import com.oracle.pic.limits.IIdentityService;
import com.oracle.pic.limits.entities.exceptions.CompartmentQuotaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Interface implementation for retrieving compartment tree for a tenancy
 */
public class CompartmentTreeRetriever implements IIdentityService {

    /**
     * Identity client
     */
    private final ResourceControlledMetadataClient metadataClient;

    /**
     * Tenant ID to namespace mapping cache
     */
    private final Cache<String, String> namespaceCache;

    private static final Logger LOG = LoggerFactory.getLogger(CompartmentTreeRetriever.class);

    public CompartmentTreeRetriever(ResourceControlledMetadataClient metadataClient,
                                    Cache<String, String> namespaceCache) {
        this.metadataClient = Preconditions.checkNotNull(metadataClient);
        this.namespaceCache = Preconditions.checkNotNull(namespaceCache);
    }

    @Override
    public CompartmentTree getCompartmentTree(String tenantId, String compartmentId)
            throws CompartmentQuotaException {
        // The caller should put the tenantID to namespace mapping in the cache prior to calling this method.
        // If the namespace can't be found in the cache, use unknown_namespace for resource controlling metadata client.
        String namespace = namespaceCache.getIfPresent(tenantId);
        if (namespace == null) {
            namespace = ResourceLimiter.UNKNOWN_NAMESPACE;
        }

        Optional<CompartmentTree> tree = metadataClient.getCompartmentTree(tenantId, namespace, false);
        if (!tree.isPresent() ||
                !tree.get().getCompartmentByOcid(compartmentId).isPresent()) {
            LOG.warn("Stale tree detected for tenantId {}; refetching tree.", tenantId);
            tree = metadataClient.getCompartmentTree(tenantId, namespace, true);
        }

        return tree.orElseThrow(() ->
                new CompartmentQuotaException("Failed to retrieve compartment tree for tenancy {}", tenantId));
    }
}
