package com.oracle.pic.casper.webserver.api.common;

import com.amazonaws.util.CollectionUtils;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.metrics.MetricScopeWriter;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetricsBundle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * A {@link Handler} that adds a {@link MetricScope} and reporting via the DropWizard Metrics library.
 *
 * This handler must be added to a {@link io.vertx.ext.web.Router} before any handlers for which metrics are to be
 * gathered. It expects downstream handlers to add a key to the {@link RoutingContext} object:
 *
 *  <code>
 *      RoutingContext context = ...;
 *      context.put(MetricsHandler.METRIC_BUNDLE_KEY, new WebServerMetricsBundle("my.prefix.goes.here");
 *  </code>
 *
 * This must be done before HttpServerResponse#end() is called, and is typically done at the start of the handler before
 * any code is exected (in case of exceptions).
 *
 * The value of the error metric is incremented on failure, the request counter is always incremented and the latency
 * metric is updated with the end-to-end latency of the request.
 */
public class  MetricsHandler implements Handler<RoutingContext> {
    private static final Logger logger = LoggerFactory.getLogger(MetricsHandler.class);

    public static final String METRIC_BUNDLE_KEY = "metric_bundle";

    private final MetricScopeWriter metricScopeWriter;
    private final long objectFilterSize;

    public MetricsHandler(MetricScopeWriter metricScopeWriter, long objectFilterSize) {
        this.metricScopeWriter = metricScopeWriter;
        this.objectFilterSize = objectFilterSize;
    }

    public static void addMetrics(RoutingContext context, WebServerMetricsBundle bundle) {
        context.put(METRIC_BUNDLE_KEY, bundle);
    }

    private void incrementRequestMetric(RoutingContext context) {
        WebServerMetricsBundle bundle = context.get(METRIC_BUNDLE_KEY);
        if (bundle != null) {
            bundle.getRequests().inc();
        }
    }

    private void incrementSuccessMetric(RoutingContext context) {
        WebServerMetricsBundle bundle = context.get(METRIC_BUNDLE_KEY);
        if (bundle != null) {
            bundle.getSuccesses().inc();
        }
    }

    private void incrementClientErrorMetric(RoutingContext context) {
        WebServerMetricsBundle bundle = context.get(METRIC_BUNDLE_KEY);
        if (bundle != null) {
            bundle.getClientErrors().inc();
        }
    }

    private void incrementRateLimitErrorMetric(RoutingContext context) {
        WebServerMetricsBundle bundle = context.get(METRIC_BUNDLE_KEY);
        if (bundle != null) {
            bundle.getRateLimitCounter().inc();
        }

    }
    private void incrementConnectionCloseMetric(RoutingContext context) {
        WebServerMetricsBundle bundle = context.get(METRIC_BUNDLE_KEY);
        if (bundle != null) {
            bundle.getConnectionClose().inc();
        }
    }

    private void incrementServerErrorMetric(RoutingContext context) {
        WebServerMetricsBundle bundle = context.get(METRIC_BUNDLE_KEY);
        if (bundle != null) {
            bundle.getServerErrors().inc();
        }
    }

    private void updateLatencyMetric(RoutingContext context, long startTimeNs) {
        WebServerMetricsBundle bundle = context.get(METRIC_BUNDLE_KEY);
        if (bundle != null) {
            bundle.getOverallLatency().update(System.nanoTime() - startTimeNs);
        }
    }

    private void updateThroughputMetric(RoutingContext context) {
        if (context.response().getStatusCode() < 200 || context.response().getStatusCode() >= 300) {
            //Filtering out failed requests
            return;
        }
        WebServerMetricsBundle bundle = context.get(METRIC_BUNDLE_KEY);
        if (bundle != null && bundle.getThroughput() != null && bundle.getInverseThroughput() != null) {
            MetricScope metricScope = WSRequestContext.getMetricScope(context);
            Collection<Object> operationList = metricScope.getAnnotations().get("operation");
            if (!CollectionUtils.isNullOrEmpty(operationList)) {
                String operation = operationList.iterator().next().toString();
                Double duration = 0.0;
                Collection<Object> durationList;
                Long bytesTransferred = 0L;
                /**
                 * totalBytesTransferred is used for PutObject requests because it describes the number of bytes
                 * transferred from the client to each of the storage servers.
                 * responseBytesWritten would generally be zero for PutObject requests,
                 * because when they succeed we return "204 No Content".
                 *
                 * For GetObject requests, of course, we're returning the object data,
                 * so responseBytesWritten is generally the number of bytes of object data that we returned
                 *
                 */
                if (operation.equals("PutObject")) {
                    bytesTransferred = context.request().bytesRead();
                    durationList = metricScope.getAnnotations().get("ttlb");
                    if (!CollectionUtils.isNullOrEmpty(durationList)) {
                        duration = (Double) durationList.iterator().next();
                    } else {
                        return;
                    }
                } else if (operation.equals("GetObject")) {
                    bytesTransferred = context.response().bytesWritten();
                    duration = metricScope.getDurationMs();
                } else {
                    return;
                }
                //Filtering out small objects.
                if (bytesTransferred < objectFilterSize) {
                    return;
                }
                Collection<Object> ttfbList = metricScope.getAnnotations().get("ttfb");
                Double ttfb;
                if (!CollectionUtils.isNullOrEmpty(ttfbList)) {
                    ttfb = (Double) ttfbList.iterator().next();
                } else {
                    return;
                }
                if (duration > 0 && ttfb > 0 && duration > ttfb) {

                    double throughput =  (bytesTransferred.doubleValue() / (duration - ttfb));
                    logger.debug("Calculating throughput: " +
                            "bytesTransferred : {} " +
                            "duration : {} " +
                            "ttfb : {} ", bytesTransferred, duration, ttfb);
                    logger.debug("throughput in bytes/millisecond : {} ", throughput);

                    double throughputInseconds = throughput * 1000;
                    logger.debug("throughput in bytes/second : {} ", throughputInseconds);
                    bundle.getThroughput().update((long) throughputInseconds);

                    // Reporting Throughput as bytes/second
                    // (throughput * 1000) -> converting bytes/millisecond to gb/sec
                    double throughputGb = throughput / 1000000;
                    logger.debug("throughput in gb/second : {} ", throughputGb);
                    double actualInverseThroughput = (1 / throughputGb);
                    logger.debug("Actual inverseThroughput (1/throughputGB)(seconds/gb): {} ", actualInverseThroughput);

                    // This inverse throughput is in millisecond/bytes
                    double inverseThroughput = (duration - ttfb) / bytesTransferred.doubleValue();
                    logger.debug("Inverse Throughput in millisecond/bytes : {} ", inverseThroughput);
                    // Converting millisecond/bytes to seconds/gigabytes (1 millisecond/byte = 1e+6 second/gigabyte)
                    double normalizedInverseThroughput =  (inverseThroughput * 1000000);
                    logger.debug("Inverse Throughput in seconds/gigabytes : {} ", normalizedInverseThroughput);
                    long normalizedInverseThroughputLong = (long) normalizedInverseThroughput;
                    logger.debug("InverseThroughput in seconds/gigabyte in long: {} ", normalizedInverseThroughputLong);

                    // Reporting the inverse throughput to calculate the P05 percentile.
                    // if P05 is greater than or equal to X seconds/gb means that 95 percent of the requests have
                    // throughput above that threshold
                    bundle.getInverseThroughput().update((long) actualInverseThroughput);

                }
            }
        }
    }

    private void logAndMetric(RoutingContext context, boolean connectionClosed) {
        final HttpServerResponse response = context.response();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);
        final CommonRequestContext commonRequestContext = wsRequestContext.getCommonRequestContext();
        final MetricScope metricScope = commonRequestContext.getMetricScope();

        if (connectionClosed || HttpResponseStatus.isClientError(response.getStatusCode())) {
            incrementClientErrorMetric(context);
        } else if (!HttpResponseStatus.isError(response.getStatusCode())) {
            incrementSuccessMetric(context);
        } else if (HttpResponseStatus.isRateLimitError(response.getStatusCode())) {
            //we don't want to count HTTP503 as a server error but its own individual metric
            incrementRateLimitErrorMetric(context);
        } else if (HttpResponseStatus.isServerError(response.getStatusCode())) {
            incrementServerErrorMetric(context);
        }

        incrementRequestMetric(context);
        updateLatencyMetric(context, wsRequestContext.getStartTime());
        updateThroughputMetric(context);
        metricScope.annotate("status", response.getStatusCode());
        metricScope.annotate("responseBytesWritten", response.bytesWritten());
        metricScope.end();
        metricScopeWriter.accept(metricScope);
    }

    @Override
    public void handle(RoutingContext context) {
        WSRequestContext wsRequestContext = WSRequestContext.get(context);
        CommonRequestContext commonRequestContext = wsRequestContext.getCommonRequestContext();
        MetricScope metricScope = commonRequestContext.getMetricScope();
        wsRequestContext.pushEndHandler(connectionClosed -> {
            if (connectionClosed) {
                // This calls the Handler installed by addBodyEndHandler below. This will not send a response back to
                // the user, and is only here to make sure we send the right metrics when the connection with the client
                // is unexpectedly closed. A closed connection can still call response.end(), but this way the metrics
                // are only logged once.
                metricScope.annotate("connectionDropped", "true");
                // update the connection metric
                incrementConnectionCloseMetric(context);

                if (!context.request().isEnded()) {
                    // we haven't finished reading the entire request and the connection is closed.
                    // In this case, it is more likely that the caller is at fault where it stopped sending us data
                    // in the middle.
                    metricScope.annotate("closeBeforeFullyConsumeRequest", "true");
                } else {
                    // we have finished reading the request but someone closes the connect before the entire response is
                    // sent. In this case, we are likely at fault here. We consume the entire request but we didn't
                    // respond to the client in time.
                    metricScope.annotate("closeBeforeFullyConsumeRequest", "false");
                }
            }
            logAndMetric(context, connectionClosed);
        });

        context.next();
    }
}
