package com.oracle.pic.casper.webserver.api.pars;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.util.ParUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * Wrapper around String for type safety.
 * Represents the full PAR ID: bucketId + '#' + Optional<objectName> + '#' + verifierId
 */
public class FullParId {

    private final String id;

    FullParId(String bucketImmutableResourceId, String objectName, String verifierId) {
        Preconditions.checkNotNull(bucketImmutableResourceId);
        Preconditions.checkNotNull(verifierId);
        this.id = Joiner.on(ParUtil.PAR_ID_SEPARATOR_BACKEND).skipNulls()
                .join(bucketImmutableResourceId, objectName, verifierId);
    }

    FullParId(String id) {
        Preconditions.checkNotNull(id);
        this.id = id;
    }

    @Override
    public String toString() {
        return this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FullParId that = (FullParId) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    BackendParId toBackendParId() {
        return new BackendParId(StringUtils.substringAfter(id, ParUtil.PAR_ID_SEPARATOR_BACKEND));
    }
}
