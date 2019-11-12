package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.common.auth.dataplane.MetadataClient;
import com.oracle.pic.casper.common.auth.dataplane.Tenant;
import com.oracle.pic.casper.webserver.limit.ResourceLimitHelper;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.limit.ResourceType;
import com.oracle.pic.commons.metadata.entities.Compartment;
import com.oracle.pic.commons.metadata.entities.CompartmentTree;
import com.oracle.pic.commons.metadata.entities.User;

import java.util.Optional;

/**
 * This class implements the resource control while accessing the MetadataClient interface.
 */
public class ResourceControlledMetadataClient {

    private final MetadataClient metadataClient;
    private final ResourceLimiter resourceLimiter;

    public ResourceControlledMetadataClient(MetadataClient metadataClient, ResourceLimiter resourceLimiter) {
        this.metadataClient = metadataClient;
        this.resourceLimiter = resourceLimiter;
    }

    public Optional<Tenant> getTenantByCompartmentId(String compartmentOcid, String namespace) {
        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY, namespace,
                () -> metadataClient.getTenantByCompartmentId(compartmentOcid));
    }

    public Optional<Compartment> getCompartment(String compartmentOcid, String namespace) {
        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY, namespace,
                () -> metadataClient.getCompartment(compartmentOcid));
    }

    public Optional<User> getUser(String userOcid, String namespace) {
        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY, namespace,
                () -> metadataClient.getUser(userOcid));
    }

    public Optional<CompartmentTree> getCompartmentTree(String compartmentOcid, String namespace, boolean refresh) {
        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY, namespace,
                () -> metadataClient.getCompartmentTree(compartmentOcid, refresh));
    }
}
