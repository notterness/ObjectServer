package com.oracle.pic.casper.webserver.api.pars;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.util.ParUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Wrapper around String for type safety.
 * Represents the customer PAR ID: verifierId + ':' + Optional<objectName>
 */
public class CustomerParId {

    private final String id;

    public CustomerParId(String id) {
        Preconditions.checkNotNull(id);
        this.id = id;
    }

    public CustomerParId(String objectName, String verifierId) {
        Preconditions.checkNotNull(verifierId);
        this.id = Joiner.on(ParUtil.PAR_ID_SEPARATOR_CUSTOMER).skipNulls().join(verifierId, objectName);
    }

    @Override
    public String toString() {
        return this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomerParId that = (CustomerParId) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    /**
     * Convert customerParId in the format of
     *
     * verifierId:objectName or verifierId (for bucket-level PARs)
     *
     * to backendParId in the format of
     *
     * objectName#verifierId or verifierId (for bucket-level PARs)
     *
     * so that {@link PreAuthenticatedRequestBackend} can retrieve it.
     *
     * e.g. =/blah+==:myAwesomeObject will become myAwesomeObject#=/blah+==
     *
     * @return parId in the backend format
     */
    BackendParId toBackendParId() {
        final String verifierId = StringUtils.substringBefore(id, ParUtil.PAR_ID_SEPARATOR_CUSTOMER);
        // verifierId == customerParId means : is not found means verifierId is backend parId means bucketPar
        if (Objects.equals(verifierId, id)) {
            return new BackendParId(id);
        }
        final String objectName = StringUtils.substringAfter(id, ParUtil.PAR_ID_SEPARATOR_CUSTOMER);
        return new BackendParId(objectName, verifierId);
    }
}
