package com.oracle.pic.casper.webserver.api.sg;

import com.google.common.primitives.Longs;
import com.oracle.pic.casper.common.config.v2.ServiceGatewayConfiguration;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * ServiceGateway is a convenience API for internal VCN IDs from socket options to VCN OCDs using a VcnIdProvider.
 *
 * Requests that arrive at Casper from the service gateway are expected to have a "x-vcn-id" header that has the
 * internal ID of the VCN from which the request was sent. The integration tests used to verify Casper use a debug
 * version of that header named "x-vcn-id-casper", and have a white-listed tenant OCID. This class provides methods
 * that choose the appropriate header, check the white-listing (if necessary) and do the mapping from internal VCN
 * ID to VCN OCID (via a VcnIdProvider).
 */
public class ServiceGateway {

    /**
     * The x-vcn-id-casper header is added to requests by our integration tests in objectstorage-tests, and the tenant
     * OCID used by those tests is white-listed (in configuration) to use that header. A request that sends this
     * header from a white-listed tenant will use it instead of x-vcn-id.
     */
    public static final String VCN_ID_CASPER_DEBUG_HEADER = "x-vcn-id-casper";

    /**
     * Identity variable name to do matching against a VCN OCID.
     */
    public static final String VCN_IDENTITY_VARIABLE = "request.vcn.id";

    private final ServiceGatewayConfiguration config;
    private final VcnIdProvider vcnIdProvider;

    public ServiceGateway(ServiceGatewayConfiguration config, VcnIdProvider vcnIdProvider) {
        this.config = Objects.requireNonNull(config);
        this.vcnIdProvider = Objects.requireNonNull(vcnIdProvider);
    }

    /**
     * Given the values of the x-vcn-id and x-vcn-id-casper headers, along with a tenant OCID, choose the ID that will
     * be mapped to a VCN OCID.
     *
     * @param vcnID the value of the x-vcn-id header, or null if there wasn't one.
     * @param vcnDebugID the value of the x-vcn-id-casper header, or null if there wasn't one.
     * @param tenantOCID the tenant OCID, for white-listing, or null if there is no tenant OCID.
     * @return an optional string that is vcnDebugID if the tenantOCID is non-null, the tenant is white-listed and the
     *         vcnDebugID was non-null. Otherwise the optional string is vcnID (which could be empty).
     */
    public Optional<String> chooseVcnID(
            @Nullable String vcnID, @Nullable String vcnDebugID, @Nullable String tenantOCID) {
        return Optional.ofNullable(isDebugWhitelistTenant(tenantOCID) && vcnDebugID != null ? vcnDebugID : vcnID);
    }

    /**
     * Map the internal VCN ID to a VCN OCID.
     *
     * @param vcnID the ID to map, typically as returned from chooseVcnID, may be null.
     * @return an empty optional if vcnID is null or there is no known mapping, or a non-empty optional that contains
     *         the mapped VCN OCID.
     */
    public Optional<String> mappingFromVcnID(@Nullable String vcnID) {
        return Optional.ofNullable(vcnID).map(Longs::tryParse).flatMap(vcnIdProvider::getMapping);
    }

    private boolean isDebugWhitelistTenant(@Nullable String tenantOcid) {
        return tenantOcid != null && config.getTenancyWhitelist().contains(tenantOcid);
    }
}
