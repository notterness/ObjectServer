package com.oracle.pic.casper.webserver.api.monitoring;

import com.amazonaws.util.CollectionUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.monitoring.MonitoringMetricsReporter;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import com.oracle.pic.telemetry.api.model.Datapoint;
import com.oracle.pic.telemetry.api.model.MetricDataDetails;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.oracle.pic.casper.common.monitoring.PublicTelemetryMetrics.PUBLIC_TELEMETRY_DROPPED_WEBSERVER_METRICS;

public class MonitoringHelper {

    private static Logger log = LoggerFactory.getLogger(MonitoringHelper.class);

    private final Clock clock;
    private MonitoringMetricsReporter metricsReporter;

    public MonitoringHelper(Clock clock, MonitoringMetricsReporter metricsReporter) {
        this.clock = clock;
        this.metricsReporter = metricsReporter;
    }

    /**
     * Upload the request for public metering.
     * Functionality to persist the metric in a log needs to be added.
     *
     * So we only need logging for requests on objects and not on buckets
     * @param context The request to work off of
     */
    public void logMonitoringMetric(final RoutingContext context) {
        try {
            final List<MetricDataDetails> metrics = createMonitoringMetrics(context);
            for (MetricDataDetails metric : metrics) {
                if (!metricsReporter.offerMetric(metric)) {
                    PUBLIC_TELEMETRY_DROPPED_WEBSERVER_METRICS.mark();
                }
            }

            WebServerMetrics.MONITORING_METRIC_SUCCESS.mark();
        } catch (Exception ex) {
            WebServerMetrics.MONITORING_METRIC_FAILURE.mark();
        }
    }

    private MetricDataDetails createMetric(String bucketName, String compartmentId,
                                           String bucketOcid, Instant timeStamp,
                                           WebServerMonitoringMetric publicMetric,
                                           double value) {
        Map<String, String> dimensions = Maps.newHashMap();
        dimensions.put(MonitoringMetricsReporter.ValidDimensions.BUCKET_NAME.getValue(), bucketName);
        dimensions.put(MonitoringMetricsReporter.ValidDimensions.BUCKET_OCID.getValue(), bucketOcid);
        return new MetricDataDetails(
                MonitoringMetricsReporter.SERVICE_NAME,
                compartmentId,
                publicMetric.getValue(),
                dimensions, publicMetric.getMetadataMap(),
                Arrays.asList(new Datapoint(Date.from(timeStamp), value, 1)));
    }

    private Optional<Double> getTTFBfromMetricsScope(MetricScope scope) {
        Collection<Object> ttfbList = scope.getAnnotations().get("ttfb");
        if (CollectionUtils.isNullOrEmpty(ttfbList)) {
            return Optional.empty();
        } else {
            return Optional.of((Double) (ttfbList.iterator().next()));
        }
    }

    private boolean verifyRequestContext(WebServerMonitoringMetric metric,
                                         String compId,
                                         String bucket,
                                         String bucketOcid,
                                         String opcReqId) {
        if (metric == null) {
            log.info("This request did not match to a matching monitoring metric to be logged {}", opcReqId);
            return false;
        }

        if (compId == null) {
            /*
             * There are cases where we might not get the compartmentId in the context and we give a 2xx response for
             * those, example:
             * https://jira.oci.oraclecorp.com/browse/CASPER-2601
             *
             * We will just return for those cases and not mark a failure. However if the compartmentId is set, then
             * bucketName and bucketOcid should be present else we mark it as a failure.
             */
            log.warn("Missing compartment ID for opc-request-id {}, will not record monitoring metric", opcReqId);
            return false;
        }

        if (bucket == null) {
            log.warn("Missing bucket name for opc-request-id {}, will not record monitoring metric", opcReqId);
            WebServerMetrics.MONITORING_METRIC_FAILURE.mark();
            return false;
        }

        if (bucketOcid == null) {
            log.warn("Missing bucket OCID for opc-request-id {}, will not record monitoring metric", opcReqId);
            WebServerMetrics.MONITORING_METRIC_FAILURE.mark();
            return false;
        }

        return true;
    }

    @VisibleForTesting
    List<MetricDataDetails> createMonitoringMetrics(RoutingContext ctx) {
        final List<MetricDataDetails> res = new ArrayList<>();

        final WSRequestContext wsCtx = WSRequestContext.get(ctx);
        final CommonRequestContext commonCtx = wsCtx.getCommonRequestContext();
        final HttpServerResponse httpRes = ctx.response();
        final String opcReqId = commonCtx.getOpcRequestId();

        final WebServerMonitoringMetric metric = wsCtx.getWebServerMonitoringMetric().orElse(null);

        final String compId = wsCtx.getCompartmentID().orElse(null);
        final String bucket = wsCtx.getBucketName().orElse(null);
        final String bucketOcid = wsCtx.getBucketOcid().orElse(null);

        if (!verifyRequestContext(metric, compId, bucket, bucketOcid, opcReqId)) {
            return res;
        }

        final boolean isClientErr = HttpResponseStatus.isClientError(httpRes.getStatusCode());
        final Optional<Double> ttfbMillis = getTTFBfromMetricsScope(commonCtx.getMetricScope());
        final long durNanos = System.nanoTime() - wsCtx.getStartTime();
        final long durMillis = TimeUnit.MILLISECONDS.convert(durNanos, TimeUnit.NANOSECONDS);
        final Instant now = clock.instant();

        res.add(createMetric(bucket, compId, bucketOcid, now, metric, 1D));
        res.add(createMetric(bucket, compId, bucketOcid, now, WebServerMonitoringMetric.ALL_REQUEST_COUNT, 1D));
        res.add(createMetric(bucket, compId, bucketOcid, now, WebServerMonitoringMetric.OVERALL_LATENCY,
                Long.valueOf(durMillis).doubleValue()));
        ttfbMillis.ifPresent(ttfb -> res.add(
                createMetric(bucket, compId, bucketOcid, now, WebServerMonitoringMetric.LATENCY_FIRSTBYTE, ttfb)));
        if (isClientErr) {
            res.add(createMetric(bucket, compId, bucketOcid, now, WebServerMonitoringMetric.ERROR_COUNT, 1D));
        }

        return res;
    }
}
