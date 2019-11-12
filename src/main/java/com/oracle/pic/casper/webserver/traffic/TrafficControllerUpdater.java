package com.oracle.pic.casper.webserver.traffic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.measure.Measure;
import javax.measure.quantity.DataRate;
import javax.measure.quantity.Duration;
import javax.measure.unit.SI;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * TrafficControllerUpdater listens for updates from a KeyValueStore and uses them to update the configuration of a
 * TrafficController.
 *
 * The updater accepts the same keys and values as TrafficControllerConfiguration (the keys are case-sensitive):
 *
 * - "TC|shadowMode" - the allowed values are the JSON booleans "true" and "false".
 *
 * - "TC|maxBandwidth" - the allowed values are strings parsable as Measure<DataRate>s.  See
 * MeasureDataRateDeserializer.
 *
 * - "TC|averageBandwidth" - the allowed values are strings parsable as Measure<DataRate>s.  See
 * MeasureDataRateDeserializer.
 *
 * - "TC|averageOverhead" - the value must be parsable as a java.time.Duration.
 *
 * - "TC|smallRequestOverheadPercentage" - the value must be parsable as double.
 *
 * - "TC|alpha" - the value must be parsable as a double.
 *
 * - "TC|tenantThrottleShadowMode" - allowed values are JSON booleans "true" and "false"
 *
 * - "TC|defaultTenantThrottlePolicy" - value must be a parsable json string to TenantThrottlePolicy POJO
 *
 * - "TC|tenantThrottlePolicyOverrides" - value must be a parsable json string to Map<String,TenantThrottlePolicy>
 *
 * The TrafficController uses the default configuration values (see TrafficControllerConfiguration) if these keys are
 * not present in the KeyValueStore. If these keys are added to the KeyValueStore with well-formed values, the
 * TrafficController uses those values. If these keys are added to the KeyValueStore with values that are not
 * well-formed, the TrafficController falls back to the default configuration values. If these keys were present in the
 * KeyValueStore and then later removed, the TrafficController falls back to the default configuration values (not the
 * most recent values from the KeyValueStore).
 */
public class TrafficControllerUpdater implements KeyValueStoreUpdater.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(TrafficControllerUpdater.class);

    private static final String KEY_PREFIX = "TC";
    static final String KEY_FOR_SHADOW_MODE = String.format("%s|shadowMode", KEY_PREFIX);
    static final String KEY_FOR_MAX_BANDWIDTH = String.format("%s|maxBandwidth", KEY_PREFIX);
    static final String KEY_FOR_AVERAGE_BANDWIDTH = String.format("%s|averageBandwidth", KEY_PREFIX);
    static final String KEY_FOR_AVERAGE_OVERHEAD = String.format("%s|averageOverhead", KEY_PREFIX);
    static final String KEY_FOR_SMALL_REQUEST_OVERHEAD_PERCENTAGE = String.format(
            "%s|smallRequestOverheadPercentage",
            KEY_PREFIX
    );
    static final String KEY_FOR_ALPHA = String.format("%s|alpha", KEY_PREFIX);

    static final String KEY_FOR_TENANT_THROTTLE_SHADOW_MODE = String.format(
            "%s|tenantThrottleShadowMode",
            KEY_PREFIX);
    static final String KEY_FOR_DEFAULT_TENANT_THROTTLE_POLICY = String.format(
            "%s|defaultTenantThrottlePolicy",
            KEY_PREFIX);
    static final String KEY_FOR_TENANT_THROTTLE_POLICY_OVERRIDES = String.format(
            "%s|tenantThrottlePolicyOverrides",
            KEY_PREFIX);
    static final String KEY_FOR_UPDATE_INTERVAL = String.format(
            "%s|updateInterval",
            KEY_PREFIX);
    static final String KEY_FOR_REQUEST_SAMPLE_PERCENTAGE = String.format(
            "%s|requestSamplePercentage",
            KEY_PREFIX);
    static final String KEY_FOR_METRICS_OF_WHITELISTED_TENANT_NAMESPACES = String.format(
            "%s|metricWhitelistedNameSpaces",
            KEY_PREFIX);


    private final boolean defaultShadowMode;
    private final boolean defaultTenantThrottleShadowMode;
    private final Measure<DataRate> defaultMaxBandwidth;
    private final Measure<DataRate> defaultAverageBandwidth;
    private final Measure<Duration> defaultAverageOverhead;
    private final double defaultAlpha;
    private final double defaultSmallRequestOverheadPercentage;
    private final Measure<Duration> defaultUpdateInterval;
    private final double defaultRequestSamplePercentage;

    private final TrafficController controller;

    static final ObjectMapper MAPPER = CasperConfig.getObjectMapper();

    public TrafficControllerUpdater(TrafficController controller) {
        this.defaultShadowMode = controller.isShadowMode();
        this.defaultTenantThrottleShadowMode = controller.getTenantThrottler().isShadowMode();
        this.defaultMaxBandwidth = controller.getMaxBandwidth();
        this.defaultAverageBandwidth = controller.getTrafficRecorderConfiguration().getAverageBandwidth();
        this.defaultAverageOverhead = controller.getTrafficRecorderConfiguration().getAverageOverhead();
        this.defaultAlpha = controller.getTrafficRecorderConfiguration().getAlpha();
        this.defaultSmallRequestOverheadPercentage = controller
                .getTrafficRecorderConfiguration()
                .getSmallRequestOverheadPercentage();
        this.defaultRequestSamplePercentage = controller.getTrafficRecorderConfiguration().getRequestSamplePercentage();
        this.defaultUpdateInterval = controller.getTrafficRecorderConfiguration().getUpdateInterval();
        this.controller = controller;
    }

    private void badValue(String key, String value) {
        LOG.debug("received a bad value for key {}: {}", key, value);
        WebServerMetrics.TC_UPDATE_ERROR.inc();
    }

    private String jsonString(String value) {
        return String.format("\"%s\"", value);
    }

    private void maybeUpdateShadow(Map<String, String> changed, Set<String> removed) {
        final String sm = changed.get(KEY_FOR_SHADOW_MODE);
        if (sm == null) {
            if (removed.contains(KEY_FOR_SHADOW_MODE)) {
                controller.setShadowMode(defaultShadowMode);
            }
            return;
        }

        try {
            controller.setShadowMode(MAPPER.readValue(sm, Boolean.class));
        } catch (IOException e) {
            badValue(KEY_FOR_SHADOW_MODE, sm);
        }
    }

    private Optional<Measure<DataRate>> parseRate(Map<String, String> changed,
                                                  Set<String> removed,
                                                  String key,
                                                  Measure<DataRate> defaultValue) {
        final String mb = changed.get(key);
        if (mb == null) {
            if (removed.contains(key)) {
                return Optional.of(defaultValue);
            }
            return Optional.empty();
        }

        try {
            final Measure<DataRate> parsed = MAPPER.readValue(
                    jsonString(mb),
                    new TypeReference<Measure<DataRate>>() {
                    }
            );
            // TODO: MeasureDataRateDeserializer should disallow negative values
            return Optional.of(parsed);
        } catch (Exception e) {
            badValue(key, mb);
            return Optional.empty();
        }
    }

    private void maybeUpdateMaxBandwidth(Map<String, String> changed, Set<String> removed) {
        parseRate(changed, removed, KEY_FOR_MAX_BANDWIDTH, defaultMaxBandwidth).ifPresent(
                controller::setMaxBandwidth
        );
    }

    private void maybeUpdateAverageBandwidth(Map<String, String> changed, Set<String> removed) {
        parseRate(changed, removed, KEY_FOR_AVERAGE_BANDWIDTH, defaultAverageBandwidth).ifPresent(
                value -> {
                    controller.getTrafficRecorderConfiguration().setAverageBandwidth(value);
                }
        );
    }

    private void maybeUpdateAverageOverhead(Map<String, String> changed, Set<String> removed) {
        final String ao = changed.get(KEY_FOR_AVERAGE_OVERHEAD);
        if (ao == null) {
            if (removed.contains(KEY_FOR_AVERAGE_OVERHEAD)) {
                controller.getTrafficRecorderConfiguration().setAverageOverhead(defaultAverageOverhead);
            }
            return;
        }
        try {
            java.time.Duration asDuration = MAPPER.readValue(
                    jsonString(ao),
                    new TypeReference<java.time.Duration>() {
                    }
            );

            long nanoseconds = asDuration.toNanos();
            if (nanoseconds < 0) {
                badValue(KEY_FOR_AVERAGE_OVERHEAD, ao);
                return;
            }

            controller.getTrafficRecorderConfiguration().setAverageOverhead(
                    Measure.valueOf(nanoseconds, SI.NANO(SI.SECOND)));
        } catch (Exception e) {
            badValue(KEY_FOR_AVERAGE_OVERHEAD, ao);
        }
    }

    private void maybeUpdateAlphaFactor(Map<String, String> changed, Set<String> removed) {
        final String a = changed.get(KEY_FOR_ALPHA);
        if (a == null) {
            if (removed.contains(KEY_FOR_ALPHA)) {
                controller.getTrafficRecorderConfiguration().setAlpha(defaultAlpha);
            }
            return;
        }
        try {
            controller.getTrafficRecorderConfiguration().setAlpha(
                    MAPPER.readValue(a, Double.class));
        } catch (Exception e) {
            badValue(KEY_FOR_ALPHA, a);
        }
    }

    private void maybeUpdateSmallRequestOverheadPercentage(Map<String, String> changed,
                                                           Set<String> removed) {
        final String srop = changed.get(KEY_FOR_SMALL_REQUEST_OVERHEAD_PERCENTAGE);
        if (srop == null) {
            if (removed.contains(KEY_FOR_SMALL_REQUEST_OVERHEAD_PERCENTAGE)) {
                controller.getTrafficRecorderConfiguration().setSmallRequestOverheadPercentage(
                        defaultSmallRequestOverheadPercentage
                );
            }
            return;
        }
        try {
            controller.getTrafficRecorderConfiguration().setSmallRequestOverheadPercentage(
                    MAPPER.readValue(srop, Double.class));
        } catch (Exception e) {
            badValue(KEY_FOR_SMALL_REQUEST_OVERHEAD_PERCENTAGE, srop);
        }
    }

    private void maybeUpdateShadowTenantThrottlePolicy(Map<String, String> changed, Set<String> removed) {
        final String sm = changed.get(KEY_FOR_TENANT_THROTTLE_SHADOW_MODE);
        if (sm == null) {
            if (removed.contains(KEY_FOR_TENANT_THROTTLE_SHADOW_MODE)) {
                controller.getTenantThrottler().setShadowMode(defaultTenantThrottleShadowMode);
            }
            return;
        }

        try {
            controller.getTenantThrottler().setShadowMode(MAPPER.readValue(sm, Boolean.class));
        } catch (IOException e) {
            badValue(KEY_FOR_TENANT_THROTTLE_SHADOW_MODE, sm);
        }
    }

    private void maybeUpdateDefaultTenantThrottlePolicy(Map<String, String> changed, Set<String> removed) {
        final String sm = changed.get(KEY_FOR_DEFAULT_TENANT_THROTTLE_POLICY);
        if (sm == null) {
            if (removed.contains(KEY_FOR_DEFAULT_TENANT_THROTTLE_POLICY)) {
                controller.getTenantThrottler().updateDefaultPolicy(null);
            }
            return;
        }

        try {
            controller.getTenantThrottler().updateDefaultPolicy(MAPPER.readValue(sm, TenantThrottlePolicy.class));
        } catch (IOException e) {
            badValue(KEY_FOR_DEFAULT_TENANT_THROTTLE_POLICY, sm);
        }
    }

    private void maybeUpdateTenantThrottlePolicyOverrides(Map<String, String> changed, Set<String> removed) {
        final String sm = changed.get(KEY_FOR_TENANT_THROTTLE_POLICY_OVERRIDES);
        if (sm == null) {
            if (removed.contains(KEY_FOR_TENANT_THROTTLE_POLICY_OVERRIDES)) {
                controller.getTenantThrottler().updatePolicyOverrides(null);
            }
            return;
        }

        try {
            controller.getTenantThrottler().updatePolicyOverrides(
                    MAPPER.readValue(sm, new TypeReference<Map<String, TenantThrottlePolicy>>() {
                    }));
        } catch (IOException e) {
            badValue(KEY_FOR_TENANT_THROTTLE_POLICY_OVERRIDES, sm);
        }
    }

    private void maybeUpdateUpdateInterval(Map<String, String> changed, Set<String> removed) {
        final String ui = changed.get(KEY_FOR_UPDATE_INTERVAL);
        if (ui == null) {
            if (removed.contains(KEY_FOR_UPDATE_INTERVAL)) {
                controller.getTrafficRecorderConfiguration().setUpdateInterval(defaultUpdateInterval);
            }
            return;
        }
        try {
            java.time.Duration asDuration = MAPPER.readValue(
                    jsonString(ui),
                    new TypeReference<java.time.Duration>() {
                    }
            );

            long millis = asDuration.toMillis();
            if (millis < 1) {
                badValue(KEY_FOR_UPDATE_INTERVAL, ui);
                return;
            }

            controller.getTrafficRecorderConfiguration().setUpdateInterval(
                    Measure.valueOf(millis, SI.MILLI(SI.SECOND)));
        } catch (Exception e) {
            badValue(KEY_FOR_UPDATE_INTERVAL, ui);
        }
    }

    private void maybeUpdateRequestSamplePercentage(Map<String, String> changed,
                                                    Set<String> removed) {
        final String rsp = changed.get(KEY_FOR_REQUEST_SAMPLE_PERCENTAGE);
        if (rsp == null) {
            if (removed.contains(KEY_FOR_REQUEST_SAMPLE_PERCENTAGE)) {
                controller.getTrafficRecorderConfiguration().setRequestSamplePercentage(
                        defaultRequestSamplePercentage
                );
            }
            return;
        }
        try {
            controller.getTrafficRecorderConfiguration().setRequestSamplePercentage(
                    MAPPER.readValue(rsp, Double.class));
        } catch (Exception e) {
            badValue(KEY_FOR_REQUEST_SAMPLE_PERCENTAGE, rsp);
        }
    }

    private void maybeUpdateMetricWhitelistedNameSpaces(Map<String, String> changed, Set<String> removed) {
        final String sm = changed.get(KEY_FOR_METRICS_OF_WHITELISTED_TENANT_NAMESPACES);
        if (sm == null) {
            if (removed.contains(KEY_FOR_METRICS_OF_WHITELISTED_TENANT_NAMESPACES)) {
                controller.getTrafficRecorderConfiguration().setMetricWhitelistedNamespaces(null);
            }
            return;
        }

        try {
            controller.getTrafficRecorderConfiguration().setMetricWhitelistedNamespaces(
                    MAPPER.readValue(sm, new TypeReference<List<String>>() {
                    }));
        } catch (IOException e) {
            badValue(KEY_FOR_METRICS_OF_WHITELISTED_TENANT_NAMESPACES, sm);
        }
    }

    @Override
    public void update(Map<String, String> changed, Set<String> removed) {
        // Log existing config
        logConfig("Existing");

        maybeUpdateShadow(changed, removed);
        maybeUpdateMaxBandwidth(changed, removed);
        maybeUpdateAverageBandwidth(changed, removed);
        maybeUpdateAverageOverhead(changed, removed);
        maybeUpdateAlphaFactor(changed, removed);
        maybeUpdateSmallRequestOverheadPercentage(changed, removed);
        maybeUpdateShadowTenantThrottlePolicy(changed, removed);
        maybeUpdateDefaultTenantThrottlePolicy(changed, removed);
        maybeUpdateTenantThrottlePolicyOverrides(changed, removed);
        maybeUpdateUpdateInterval(changed, removed);
        maybeUpdateRequestSamplePercentage(changed, removed);
        maybeUpdateMetricWhitelistedNameSpaces(changed, removed);

        // Log modified and final config
        logConfig("Updated");
    }

    private void logConfig(String prefix) {
        LOG.info("{} TrafficController Config: ShadowMode={}, MaxBandwidth={}",
                prefix,
                controller.isShadowMode(),
                controller.getMaxBandwidth());
        LOG.info("{} TrafficRecorder Config: {}",
                prefix,
                controller.getTrafficRecorderConfiguration().toString());
        LOG.info("{} TenantThrottler Config: {}",
                prefix,
                controller.getTenantThrottler().toString());
    }
}
