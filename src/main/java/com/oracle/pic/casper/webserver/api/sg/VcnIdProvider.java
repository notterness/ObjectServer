package com.oracle.pic.casper.webserver.api.sg;

import java.util.Optional;

/**
 * Casper will be integrated with <a href="https://confluence.oci.oraclecorp.com/display/CASPER/Service+Gateway">
 *     Service Gateway</a>. Service Gateways allow customers to access Casper from VCN without internet access.
 * In order to support authorization based on VCN, the OCID from which the request originated needs to be known.
 * Service Gateway itself cannot inject into the TCP/IP payload a full OCID (due to limitation on size) but rather
 * a 64bit identifier.
 * The {@link VcnIdProvider} will return the mapped OCID for the identifier provided.
 */
public interface VcnIdProvider {

    /**
     * Return the OCID for a particular VNC ID
     * The mapping is surfaced from the service gateway client.
     * @param vcnId The vcn id that is set in the HTTP header by flamingo
     * @return The ocid for the VCN
     */
    Optional<String> getMapping(long vcnId);

    default void start() {
        // start does nothing by default.
    }

    default void stop() {
        // stop does nothing by default.
    }

    /**
     * Simple provider that always returns {@link Optional#empty()}
     */
    static VcnIdProvider emptyProvider() {
        return vcnId -> Optional.empty();
    }

    /**
     * Simple provider that always returns the fixed id given.
     */
    static VcnIdProvider fixedProvider(String fixed) {
        return id -> Optional.of(fixed);
    }

}
