package com.oracle.pic.casper.webserver.traffic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * This POJO represents a tenant level throttle policy. Once tenant bandwidth exceeds min threshold, throttler starts
 * weighted random rejections till max threshold. Post max threshold, all requests would be rejected for that tenant!
 */

public class TenantThrottlePolicy {
    @JsonProperty("minBandwidthThresholdPercent")
    private final int minBandwidthThresholdPercent;

    @JsonProperty("maxBandwidthThresholdPercent")
    private final int maxBandwidthThresholdPercent;

    public TenantThrottlePolicy() {
        this.maxBandwidthThresholdPercent = 100;
        this.minBandwidthThresholdPercent = 100;
    }

    @JsonCreator
    public TenantThrottlePolicy(@JsonProperty("minBandwidthThresholdPercent") int minBandwidthThresholdPercent,
                                @JsonProperty("maxBandwidthThresholdPercent") int maxBandwidthThresholdPercent) {
        this.minBandwidthThresholdPercent = minBandwidthThresholdPercent;
        this.maxBandwidthThresholdPercent = maxBandwidthThresholdPercent;
    }

    public int getMinBandwidthThresholdPercent() {
        return minBandwidthThresholdPercent;
    }

    public int getMaxBandwidthThresholdPercent() {
        return maxBandwidthThresholdPercent;
    }

    public boolean isValid() {
        return validPercentVal(minBandwidthThresholdPercent)
                && validPercentVal(maxBandwidthThresholdPercent)
                && maxBandwidthThresholdPercent >= minBandwidthThresholdPercent;
    }

    private boolean validPercentVal(int p) {
        return p >= 0
                && p <= 100;
    }

    @Override
    public String toString() {
        return "TenantThrottlePolicy{" +
                "minBandwidthThresholdPercent=" + minBandwidthThresholdPercent +
                ", maxBandwidthThresholdPercent=" + maxBandwidthThresholdPercent +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TenantThrottlePolicy that = (TenantThrottlePolicy) o;
        return minBandwidthThresholdPercent == that.minBandwidthThresholdPercent &&
                maxBandwidthThresholdPercent == that.maxBandwidthThresholdPercent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minBandwidthThresholdPercent, maxBandwidthThresholdPercent);
    }
}
