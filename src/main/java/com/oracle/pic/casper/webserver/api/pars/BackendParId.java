package com.oracle.pic.casper.webserver.api.pars;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.util.ParUtil;
import com.oracle.pic.casper.webserver.api.auth.ParSigningHelper;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Wrapper around String for type safety.
 * Represents the backend PAR ID: Optional<objectName> + '#' + verifierId
 */
public class BackendParId {

    private static final Logger LOG = LoggerFactory.getLogger(BackendParId.class);

    private final String id;

    public BackendParId(String id) {
        Preconditions.checkNotNull(id);
        this.id = id;
    }

    public BackendParId(String objectName, String verifierId) {
        Preconditions.checkNotNull(verifierId);
        this.id = Joiner.on(ParUtil.PAR_ID_SEPARATOR_BACKEND).skipNulls().join(objectName, verifierId);
    }

    public BackendParId(String namespace, String bucketName, @Nullable String objectName, String nonce) {
        LOG.debug("Constructing backendParId from namespace {}, bucketName {}, objectName {}, and nonce [SANITIZED]",
            namespace, bucketName, objectName == null ? "null" : objectName);
        final Map<String, String> sts = ParSigningHelper.getSTS(namespace, bucketName, objectName);
        final String verifierId = ParSigningHelper.getVerifierId(sts, nonce);
        // After upgrade to Guava 25.1-jre, mvn:spotbugs needs an explicit check for null on every @Nullable parameter
        // passed in.
        if (objectName == null) {
            this.id = Joiner.on(ParUtil.PAR_ID_SEPARATOR_BACKEND).skipNulls().join(null, verifierId);
        } else {
            this.id = Joiner.on(ParUtil.PAR_ID_SEPARATOR_BACKEND).skipNulls().join(objectName, verifierId);
        }
    }

    @Override
    public String toString() {
        return this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BackendParId that = (BackendParId) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    FullParId toFullParId(String bucketImmutableResourceId) {
        return new FullParId(Joiner.on(ParUtil.PAR_ID_SEPARATOR_BACKEND).join(bucketImmutableResourceId, id));
    }

    /**
     * Convert the backendParId in {@link PreAuthenticatedRequestMetadata} in the format of
     *
     * objectName#verifierId or verifierId (for bucket-level PARs)
     *
     * to customerParId in the format of
     *
     * verifierId:objectName or verifierId (for bucket-level PARs)
     *
     * because we don't want # to be in URIs.
     *
     * e.g. myAwesomeObject#=/blah+== will become =/blah+==:myAwesomeObject
     *
     * @return parId in the customer format
     */
    CustomerParId toCustomerParId() {
        // should be >= 1 for object-level PARs or 0 for bucket-level PARs
        int count = StringUtils.countMatches(id, ParUtil.PAR_ID_SEPARATOR_BACKEND);
        // no # means no objectName means bucket-level PAR means backend parId is verifierId, just return
        if (count == 0) {
            return new CustomerParId(id);
        }
        final String objectName = StringUtils.substringBeforeLast(id, ParUtil.PAR_ID_SEPARATOR_BACKEND);
        final String verifierId = StringUtils.substringAfterLast(id, ParUtil.PAR_ID_SEPARATOR_BACKEND);
        return new CustomerParId(objectName, verifierId);
    }
}
