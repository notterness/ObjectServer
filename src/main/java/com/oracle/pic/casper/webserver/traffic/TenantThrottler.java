package com.oracle.pic.casper.webserver.traffic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Tenant Throttler owns the control for tenant level throttling based on the policies. It works with a policy that has
 * a configurable minimum percent threshold and a configurable maximum percent threshold.
 * If any tenant's request falls below the minimum percent threshold, its accepted
 * If any tenant's request falls above the maximum percent threshold, its rejected
 * If the tenant's current bandwidth consumption is between min and max thresholds, it uses weighted random approach to
 * decide to accept request or reject request!
 *
 * There are two ways to set tenant throttle policy:
 * 1. Default WebServer level policy that's applicable to all non-null tenantNameSpaces.
 * 2. Per Tenant Namespace override to give differential treatment to different tenants.
 *
 * Tenant Throttler could be configured to run in shadow mode, which would just enable a new set of metrics that would
 * help in evaluating the behavior of tenant level throttling!
 *
 */

public class TenantThrottler {
    private static final Logger LOG = LoggerFactory.getLogger(TenantThrottler.class);
    private final SecureRandom random;

    static final TenantThrottlePolicy UNSET_DEFAULT_THROTTLE_POLICY = new TenantThrottlePolicy();
    static final ImmutableMap<String, TenantThrottlePolicy> UNSET_TENANT_THROTTLE_POLICY_OVERRIDES = ImmutableMap.of();

    private volatile TenantThrottlePolicy defaultTenantThrottlePolicy;
    private volatile ImmutableMap<String, TenantThrottlePolicy> tenantThrottlePolicyOverrides;

    private volatile boolean shadowMode;

    public TenantThrottler(boolean shadowMode) {
        this.shadowMode = shadowMode;
        this.defaultTenantThrottlePolicy = UNSET_DEFAULT_THROTTLE_POLICY;
        this.tenantThrottlePolicyOverrides = UNSET_TENANT_THROTTLE_POLICY_OVERRIDES;
        this.random = new SecureRandom();
    }

    @VisibleForTesting
    TenantThrottler(SecureRandom random, TenantThrottlePolicy defaultTenantThrottlePolicy,
                    ImmutableMap<String, TenantThrottlePolicy> tenantThrottlePolicyOverrides, boolean shadowMode) {
        this.random = random;
        this.defaultTenantThrottlePolicy = defaultTenantThrottlePolicy;
        this.tenantThrottlePolicyOverrides = tenantThrottlePolicyOverrides;
        this.shadowMode = shadowMode;
    }

    public boolean isShadowMode() {
        return shadowMode;
    }

    @VisibleForTesting
    TenantThrottlePolicy getDefaultTenantThrottlePolicy() {
        return defaultTenantThrottlePolicy;
    }

    @VisibleForTesting
    ImmutableMap<String, TenantThrottlePolicy> getTenantThrottlePolicyOverrides() {
        return tenantThrottlePolicyOverrides;
    }

    @VisibleForTesting
    TenantThrottlePolicy getTenantPolicy(String tenantNameSpace) {
        return tenantThrottlePolicyOverrides.getOrDefault(tenantNameSpace, defaultTenantThrottlePolicy);
    }

    /**
     * Called by TrafficController to decide on whether a particular request need to be throttled or not depending on
     * the usage tracked at tenant namespace
     *
     * @param tenantNameSpace               Tenant Namespace
     * @param requestType                   PutObject or GetObject
     * @param estTenantBWUsageInBytesPerSec Usage against a tenant namespace including all accepted requests and
     *                                      the request in flight
     * @param maxBandwidthRateInBytesPerSec Per Webserver limit
     * @return True if request could be accepted
     */
    @VisibleForTesting
    boolean shouldAcceptRequest(String tenantNameSpace, TrafficRecorder.RequestType requestType,
                                long estTenantBWUsageInBytesPerSec,
                                long maxBandwidthRateInBytesPerSec) {
        if (tenantNameSpace == null) {
            return true;
        }

        TenantThrottlePolicy finalPolicy = getTenantPolicy(tenantNameSpace);

        boolean result = canAccept(estTenantBWUsageInBytesPerSec,
                maxBandwidthRateInBytesPerSec,
                finalPolicy.getMinBandwidthThresholdPercent(),
                finalPolicy.getMaxBandwidthThresholdPercent());

        switch (requestType) {
            case GetObject:
                // Update right get metrics
                if (result) {
                    WebServerMetrics.TC_TT_GET_ACCEPT.inc();
                } else {
                    LOG.warn("Excessive usage: Tenant={}, EstimatedBW={}, MaxBW={}",
                            tenantNameSpace,
                            estTenantBWUsageInBytesPerSec,
                            maxBandwidthRateInBytesPerSec);
                    WebServerMetrics.TC_TT_GET_REJECT.inc();
                }
                break;
            case PutObject:
                // Update right put metrics
                if (result) {
                    WebServerMetrics.TC_TT_PUT_ACCEPT.inc();
                } else {
                    LOG.warn("Excessive usage: Tenant={}, EstimatedBW={}, MaxBW={}",
                            tenantNameSpace,
                            estTenantBWUsageInBytesPerSec,
                            maxBandwidthRateInBytesPerSec);
                    WebServerMetrics.TC_TT_PUT_REJECT.inc();
                }

                break;
            default:
                throw new IllegalArgumentException(String.format("Invalid request type:%s", requestType));
        }

        return shadowMode || result;
    }

    /**
     * Invoked by TrafficControllerUpdater when there is an update to tenantthroller shadow mode in operator DB
     * @param shadowMode
     */
    public void setShadowMode(boolean shadowMode) {
        this.shadowMode = shadowMode;
    }

    /**
     * Invoked by TrafficControllerUpdater when there is an update to default tenant policy in operator DB
     * @param defaultPolicy WebServer level default throttle policy that would be used if there are no overrides
     */
    public void updateDefaultPolicy(TenantThrottlePolicy defaultPolicy) {
        LOG.info("DefaultPolicy {}", defaultPolicy);

        if (defaultPolicy == null) {
            this.defaultTenantThrottlePolicy = UNSET_DEFAULT_THROTTLE_POLICY;
        } else if (defaultPolicy.isValid()) {
            this.defaultTenantThrottlePolicy = defaultPolicy;
        } else {
            LOG.warn("Invalid default throttle policy supplied {}. Leaving the current default policy as is {}",
                    defaultPolicy,
                    this.defaultTenantThrottlePolicy);
        }
    }

    /**
     * Invoked by TrafficControllerUpdater when there is an update to throttle policy overrides
     * @param policyOverrides Key is tenant namespace and value is of type TenantThrottlePolicy
     */
    public void updatePolicyOverrides(Map<String, TenantThrottlePolicy> policyOverrides) {
        LOG.info("PolicyOverrides {}", policyOverrides);
        if (policyOverrides == null) {
            tenantThrottlePolicyOverrides = UNSET_TENANT_THROTTLE_POLICY_OVERRIDES;
        } else if (policyOverrides.entrySet().stream().noneMatch(e -> !e.getValue().isValid())) {
            this.tenantThrottlePolicyOverrides = ImmutableMap.copyOf(policyOverrides);
        } else {
            LOG.warn("Invalid throttle policy overrides {}. Leaving existing overrides as is {}",
                    policyOverrides,
                    this.tenantThrottlePolicyOverrides);
        }
    }

    /**
     * Given max rate supported by a webserver, estimated rate of the request apply the weighted random
     * rule and decide if the request could be accepted!
     */
    private boolean canAccept(long estRate, long maxRate, int minPercent, int maxPercent) {
        int percentUtilization = (int) ((estRate * 100) / maxRate);
        if (percentUtilization <= minPercent || minPercent == 100) {
            return true;
        }

        if (percentUtilization > maxPercent) {
            return false;
        }

        if (maxPercent > minPercent) {
            int rand = random.nextInt(maxPercent - minPercent);
            return rand + minPercent > percentUtilization;
        } else {
            return percentUtilization <= minPercent;
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TenantThrottler.class.getSimpleName() + "[", "]")
                .add("defaultTenantThrottlePolicy=" + defaultTenantThrottlePolicy)
                .add("tenantThrottlePolicyOverrides=" + tenantThrottlePolicyOverrides)
                .add("shadowMode=" + shadowMode)
                .toString();
    }
}
