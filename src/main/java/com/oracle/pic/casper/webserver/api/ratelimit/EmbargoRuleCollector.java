package com.oracle.pic.casper.webserver.api.ratelimit;

import com.oracle.pic.casper.mds.operator.MdsEmbargoRuleV1;

import java.util.Collection;

/**
 * Generic interface for some service that collects our rate limiting and embargo rules
 */
public interface EmbargoRuleCollector {

    String MAX_PUT_GET_REQUESTS_SHADOW_MODE = "maxGetPutRequestsShadowMode";

    @Deprecated
    Collection<MdsEmbargoRuleV1> getEmbargoRules();

    /**
     * Get dry-run rules. These should be evaluated, and metrics emitted, but
     * no requests should be rejected.
     */
    Collection<EmbargoV3Rule> getDryRunRules();

    /**
     * Get embargo rules.
     */
    Collection<EmbargoV3Rule> getEnabledRules();

    /**
     * If the implementations have to shutdown gracefully, they can override this method!
     */
    default void shutdown() {
    }
}
