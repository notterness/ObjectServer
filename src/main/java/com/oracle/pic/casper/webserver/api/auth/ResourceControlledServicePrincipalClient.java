package com.oracle.pic.casper.webserver.api.auth;

import com.oracle.pic.casper.webserver.limit.ResourceLimitHelper;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.limit.ResourceType;
import com.oracle.pic.casper.webserverv2.client.ServicePrincipalClientFactory;
import com.oracle.pic.identity.authentication.Principal;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * This class implements the resource control while accessing the ServicePrincipalClientFactory interface.
 */
public class ResourceControlledServicePrincipalClient {

    private final ServicePrincipalClientFactory servicePrincipalClientFactory;
    private final ResourceLimiter resourceLimiter;

    public ResourceControlledServicePrincipalClient(ServicePrincipalClientFactory servicePrincipalClientFactory,
                                                    ResourceLimiter resourceLimiter) {
        this.servicePrincipalClientFactory = servicePrincipalClientFactory;
        this.resourceLimiter = resourceLimiter;
    }

    public String getOboToken(String method, String path, Map<String, List<String>> headers,
                              Principal originalRequestPrincipal, @Nullable Principal oboCallerPrincipal,
                              String namespace) {
        return ResourceLimitHelper.withResourceControl(resourceLimiter, ResourceType.IDENTITY, namespace,
                () -> servicePrincipalClientFactory.getOboToken(
                        method, path, headers, originalRequestPrincipal, oboCallerPrincipal));
    }
}
