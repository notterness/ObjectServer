package com.oracle.pic.casper.webserver.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Optional;

/**
 * Tenants used by tests.
 */
public class TestTenants {
    private Map<String, String> debugTenantsByOCID = null;

    /**
     * Create a collection of test tenants.
     * @param debugTenantsByName A mapping of human friendly names (e.g. BMCOSTESTS) to tenant OCIDs.
     */
    public TestTenants(@NotNull Map<String, String> debugTenantsByName) {
        Preconditions.checkNotNull(debugTenantsByName);
        this.debugTenantsByOCID = createDebugTenantsByOCID(debugTenantsByName);
    }

    /**
     * Reverse a mapping of human friendly names to tenant OCIDs.
     * @param debugTenantsByName A map of human friendly names to tenant OCIDs.
     * @return
     */
    private static Map<String, String> createDebugTenantsByOCID(Map<String, String> debugTenantsByName) {
        ImmutableMap.Builder<String, String> byOCIDBuilder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : debugTenantsByName.entrySet()) {
            byOCIDBuilder.put(entry.getValue(), entry.getKey());
        }
        return byOCIDBuilder.build();
    }

    /**
     * Return the human friendly name (e.g. BMCOSTESTS) for a tenant OCID if it represents a known test tenant.
     * @param ocid The OCID
     * @return An optional that wraps the name if the OCID represents a test tenant and is empty if it does not.
     */
    public Optional<String> tenantName(String ocid) {
        return Optional.ofNullable(debugTenantsByOCID.get(ocid));
    }
}
