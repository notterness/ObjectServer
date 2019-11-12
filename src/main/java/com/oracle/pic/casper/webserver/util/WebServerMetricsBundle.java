package com.oracle.pic.casper.webserver.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.google.common.base.Objects;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.MetricsBundle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A bundle of metrics used by each of the web server API endpoints.
 *
 * The bundled metrics include all the metrics in {@link MetricsBundle}.
 */
public class WebServerMetricsBundle extends MetricsBundle {

    private static final String CONNECTION_CLOSE_SUFFIX = ".connection.close";
    public static final String SERVER_TOO_BUSY_SUFFIX = ".http.503";
    private static final String THROUGHPUT_SUFFIX = ".throughput";
    private static final String INVERSE_THROUGHPUT_SUFFIX = ".inverse.throughput";

    private final Counter connectionClose;

    /**
     * A counter for the number of HTTP 503 responses returned. As 503's are neither server nor client error
     * we do not want to count them towards availability as it adversely impacts our availability metrics.
     * Thus introducing the variable here to add this count while calculating availability.
     */
    private final Counter rateLimitCounter;

    /**
     *A histogram for emitting last byte latencies for PUT/GET requests as bytes/second
     */
    private Histogram throughput;

    /**
     *A histogram for emitting last byte latencies for PUT/GET requests as seconds/bytes
     */
    private Histogram inverseThroughput;

    /**
     * Constructor.
     *
     * This names the metrics as described in {@link MetricsBundle}.
     *
     * @param prefix the prefix to use for the metrics in the bundle.
     */
    public WebServerMetricsBundle(String prefix) {
        super(prefix);
        connectionClose = Metrics.counter(prefix + CONNECTION_CLOSE_SUFFIX);
        rateLimitCounter = Metrics.counter(prefix + SERVER_TOO_BUSY_SUFFIX);
        Metrics.percentageHistogram(prefix + SERVER_TOO_BUSY_SUFFIX, rateLimitCounter, getRequests());
    }

    /**
     * Constructor.
     *
     * This names the metrics as described in {@link MetricsBundle}.
     *
     * @param prefix             the prefix to use for the metrics in the bundle.
     * @param firstByteLatencies if true, create first-byte overallLatency metrics as well
     */
    public WebServerMetricsBundle(String prefix, boolean firstByteLatencies) {
        super(prefix, firstByteLatencies);
        connectionClose = Metrics.counter(prefix + CONNECTION_CLOSE_SUFFIX);
        rateLimitCounter = Metrics.counter(prefix + SERVER_TOO_BUSY_SUFFIX);
        Metrics.percentageHistogram(prefix + SERVER_TOO_BUSY_SUFFIX, rateLimitCounter, getRequests());
    }

    /**
     * Constructor.
     *
     * This names the metrics as described in {@link MetricsBundle}.
     *
     * @param prefix             the prefix to use for the metrics in the bundle.
     * @param firstByteLatencies if true, create first-byte overallLatency metrics as well
     * @param throughputs if true, create throughput metrics
     */
    public WebServerMetricsBundle(String prefix, boolean firstByteLatencies, boolean throughputs) {
        this(prefix, firstByteLatencies);
        if (throughputs) {
            throughput = Metrics.unaggregatedHistogram(prefix + THROUGHPUT_SUFFIX);
            inverseThroughput = Metrics.unaggregatedHistogram(prefix + INVERSE_THROUGHPUT_SUFFIX);
        }
    }

    @Nonnull
    public Counter getConnectionClose() {
        return connectionClose;
    }

    @Nonnull
    public Counter getRateLimitCounter() {
        return rateLimitCounter;
    }

    @Nullable
    public Histogram getThroughput() {
        return throughput;
    }

    public Histogram getInverseThroughput() {
        return inverseThroughput;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        WebServerMetricsBundle that = (WebServerMetricsBundle) o;
        return Objects.equal(connectionClose, that.connectionClose) &&
                Objects.equal(throughput, that.throughput) &&
                Objects.equal(inverseThroughput, that.inverseThroughput);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), connectionClose, throughput);
    }

    @Override
    public String toString() {
        return "WebServerMetricsBundle{} " + super.toString() +
        ", throughput=" + throughput +
        ", inverseThroughput=" + inverseThroughput;
    }
}
