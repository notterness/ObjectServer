package com.oracle.pic.casper.webserver.traffic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.telemetry.commons.metrics.model.MetricName;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.measure.Measure;
import javax.measure.quantity.DataRate;
import javax.measure.quantity.Duration;
import javax.measure.unit.SI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TrafficRecorder maintains an estimate for the bandwidth usage of every PUT and GET object request currently active in
 * the web server.
 *
 * TrafficRecorder instances are not thread-safe, and modifications should only be made by a single Vert.x event
 * loop thread.  Read-only operations such as {@link #getBandwidthUsage()} and {@link #getTenantBandwidthUsage(String)},
 * however, can be safely called from other threads.
 * The TrafficController maintains a list of all the TrafficRecorders in the web server, and uses these read-only
 * methods to generate a global estimate of the bandwidth being used by GET and PUT object requests in the web server
 * (see the TrafficController class docs for details).
 *
 * The TrafficRecorder uses a simple algorithm for estimating the bandwidth usage of each request:
 *
 * 1. When the request arrives, the content-length is used to compute a (rough) estimate of the expected bandwidth
 * usage. This is done using the expectedBandwidth() method, and requires hand-tuned parameters (averageBandwidth,
 * averageOverhead, and smallRequestOverheadPercentage) that are subject to change. It is not important that this
 * estimate be exact, and it is better if it is an overestimate, except in the case of short-lived requests that will
 * never achieve the average bandwidth. A request is likely to be short-lived when its lifetime is dominated by
 * averageOverhead.  The starting time of the request is stored for later use. Also, the requests are tracked against
 * tenant namespace to enable tenant level throttling!
 *
 * 2. When a buffer arrives for the request (either from the client for a PUT request, or from a storage server for a
 * GET request), the total number of bytes received by the request is updated and stored both at an aggregate level
 * and under each tenantNameSpace as well!
 *
 * 3. When the "update()" method of the TrafficRecorder is called, the "observed" bandwidth of the request is computed
 * as:
 *
 *  obsRate = newBytesReceived / (currentTime - lastUpdateTime)
 *
 * 4. The observed bandwidth and the previous estimated bandwidth are averaged using an EWMA with an alpha of .5.  The
 * estimate semantics are the same for per tenant namespace as well!
 *
 *  rateEstimate = alpha*(obsRate) + (1-alpha)*rateEstimate.
 *
 * 5. The TrafficRecorders sums the rates of all requests to compute the "total" rate being used by the event loop with
 * which it is associated (and the TrafficController sums over all TrafficRecorders).
 *
 * There are some reasons for having an initial estimate of the request. First, we need an immediate estimate of the
 * bandwidth usage of a request, and we can't wait for data to arrive before we compute it. That is due to the fact
 * that multiple requests can arrive at around the same time (particularly after GC pauses); without an initial
 * estimate, we would allow all of these through and overcommit the server. Second, it can take some time for
 * requests to ramp up to full speed. That is due to two reasons:
 *
 * 1. the delay that occurs while we talk to identity and the metadata database;
 *
 * 2. and the delay that occurs due to TCP slow start (on new connections).
 *
 * By blending in the estimate, we smooth over that delay, and avoid cases where we've added too many new slow moving
 * connections that will speed up over time (causing brownout).
 *
 * These two factors also explain why short-lived, small requests must have their bandwidth under-estimated; reason #1,
 * when taken with large TCP receive and send buffers, means that a large portion of a PutObject request's payload
 * might have arrived before the traffic controller learns about it, while reason #2 means that a GetObject or
 * PutObject request can send a large portion of its data before TCP loosens its congestion protection.
 *
 * See {@link com.oracle.pic.casper.webserver.server.WebServerVerticle#start()} for how this class is wired-up with the
 * Vert.x event loops (and how we periodically call the update() method of this class).
 *
 * Finally, a note about units: the TrafficRecorder tracks bandwidth internally as bytes per nanosecond, while the
 * {@link TrafficController} works in terms of bytes per second for reporting and limiting.
 */
public class TrafficRecorder {

    private static Logger trafficLog = LoggerFactory.getLogger("com.oracle.pic.casper.webserver.traffic_log");

    static Logger getTrafficLog() {
        return trafficLog;
    }

    static void setTrafficLog(Logger trafficLog) {
        TrafficRecorder.trafficLog = trafficLog;
    }

    /**
     * The key used to store a TrafficRecorder in a Vert.x context.
     */
    private static final String RECORDER_CONTEXT_KEY = "traffic_recorder";

    /**
     * Associate a {@link TrafficRecorder} with a single Vert.x event loop by placing it in the current {@link
     * io.vertx.core.Context}
     *
     * @param recorder The {@link TrafficRecorder} to associate with the current Vert.x context.
     */
    public static void setVertxRecorder(TrafficRecorder recorder) {
        Map<String, String> dimensions = ImmutableMap.of("thread", Thread.currentThread().getName());
        recorder.bandwidthGuage = Metrics.gauge(MetricName.of("ws.tc.recorder.bandwidth", dimensions));
        recorder.concurrencyGuage = Metrics.gauge(MetricName.of("ws.tc.recorder.concurrency", dimensions));
        Vertx.currentContext().put(RECORDER_CONTEXT_KEY, recorder);
    }

    /**
     * Return the {@link TrafficRecorder} associated with the current {@link io.vertx.core.Context}.
     *
     * @return The {@link TrafficRecorder} instance associated with the current {@link io.vertx.core.Context}.
     */
    public static TrafficRecorder getVertxRecorder() {
        TrafficRecorder recorder = Vertx.currentContext().get(RECORDER_CONTEXT_KEY);
        assert recorder != null;
        return recorder;
    }

    public static class Configuration {
        /**
         * A rough estimate for the average bandwidth used by each request.
         */
        private volatile Measure<DataRate> averageBandwidth;

        /**
         * A rough estimate of the time spent in overhead, including database queries, authn/authz, etc. This estimate
         * is taken from web server logs under high load, so it is an overstimate for normal traffic (but that is
         * expected and desired, as the TrafficRecorder will rapidly converge on a better estimate).
         *
         * In nanoseconds.
         */
        private volatile Measure<Duration> averageOverhead;

        /**
         * A number that determines how quickly the moving average changes. A higher alpha means that weight will be
         * placed more on the most recent updates. Should be within (0,1], where 1 indicates only using data from the
         * most recent update.
         */
        private volatile double alpha;

        /**
         * A small request is one whose lifetime is dominated by the {@link Configuration#averageOverhead}, because such
         * a request can transmit most, if not all, of its payload while it is being accepted.  This variable
         * determines the maximum percentage of a request's lifetime that can be spent in overhead before it is
         * classified as a small request.  See
         * {@link TrafficRecorder#expectedBandwidth(RequestType, long, Configuration)}
         */
        private volatile double smallRequestOverheadPercentage;

        /**
         * The time interval between two updates for the traffic recorder.
         */
        private volatile Measure<Duration> updateInterval;

        /**
         * The percentage (0 - 100) of requests that will be logged in the traffic log (this should be low
         * to avoid a lot of logging overhead).
         */
        private volatile double requestSamplePercentage;

        private volatile Set<String> metricWhitelistedNamespaces;

        public Configuration(Measure<DataRate> averageBandwidth,
                             Measure<Duration> averageOverhead,
                             double alpha,
                             double smallRequestOverheadPercentage,
                             Measure<Duration> updateInterval,
                             double requestSamplePercentage) {
            this.averageBandwidth = averageBandwidth;
            this.averageOverhead = averageOverhead;
            this.alpha = alpha;
            this.smallRequestOverheadPercentage = smallRequestOverheadPercentage;
            this.updateInterval = updateInterval;
            this.requestSamplePercentage = requestSamplePercentage;
            this.metricWhitelistedNamespaces = ImmutableSet.of();
        }

        /**
         * Copy constructor.
         *
         * @param copyFrom The {@link Configuration} to copy.
         */
        Configuration(Configuration copyFrom) {
            this(
                    copyFrom.getAverageBandwidth(),
                    copyFrom.getAverageOverhead(),
                    copyFrom.getAlpha(),
                    copyFrom.getSmallRequestOverheadPercentage(),
                    copyFrom.getUpdateInterval(),
                    copyFrom.getRequestSamplePercentage()
            );
        }

        public Measure<DataRate> getAverageBandwidth() {
            return averageBandwidth;
        }

        public void setAverageBandwidth(Measure<DataRate> averageBandwidth) {
            this.averageBandwidth = averageBandwidth;
        }

        public Measure<Duration> getAverageOverhead() {
            return averageOverhead;
        }

        public void setAverageOverhead(Measure<Duration> averageOverhead) {
            this.averageOverhead = averageOverhead;
        }

        public double getAlpha() {
            return alpha;
        }

        public void setAlpha(double alpha) {
            this.alpha = alpha;
        }

        public double getSmallRequestOverheadPercentage() {
            return smallRequestOverheadPercentage;
        }

        public void setSmallRequestOverheadPercentage(double smallRequestOverheadPercentage) {
            this.smallRequestOverheadPercentage = smallRequestOverheadPercentage;
        }

        public Measure<Duration> getUpdateInterval() {
            return updateInterval;
        }

        public void setUpdateInterval(Measure<Duration> updateInterval) {
            this.updateInterval = updateInterval;
        }

        public double getRequestSamplePercentage() {
            return requestSamplePercentage;
        }

        public void setRequestSamplePercentage(double requestSamplePercentage) {
            this.requestSamplePercentage = requestSamplePercentage;
        }

        public Set<String> getMetricWhitelistedNamespaces() {
            return metricWhitelistedNamespaces;
        }

        public void setMetricWhitelistedNamespaces(Collection<String> metricWhitelistedNamespaces) {
            if (metricWhitelistedNamespaces == null) {
                this.metricWhitelistedNamespaces = ImmutableSet.of();
            } else {
                this.metricWhitelistedNamespaces = ImmutableSet.copyOf(metricWhitelistedNamespaces);
            }
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Configuration.class.getSimpleName() + "[", "]")
                    .add("averageBandwidth=" + averageBandwidth)
                    .add("averageOverhead=" + averageOverhead)
                    .add("alpha=" + alpha)
                    .add("smallRequestOverheadPercentage=" + smallRequestOverheadPercentage)
                    .add("updateInterval=" + updateInterval)
                    .add("requestSamplePercentage=" + requestSamplePercentage)
                    .add("metricWhitelistedTenancies=" + metricWhitelistedNamespaces)
                    .toString();
        }
    }

    @VisibleForTesting
    static long rateMultiplier(RequestType type) {
        return type == RequestType.PutObject ? 3 : 1;
    }

    /**
     * Returns the expected bandwidth usage of a request with the given Content-Length.
     *
     * @param contentLength The request's content length
     *
     * @return A {@link Measure<DataRate>} representing the expected bandwidth.
     */
    private static Measure<DataRate> expectedBandwidthRate(RequestType type,
                                                           long contentLength,
                                                           Configuration configuration) {
        Measure<Duration> averageOverhead = configuration.getAverageOverhead();
        Measure<DataRate> averageBandwidth = configuration.getAverageBandwidth();
        double overheadPercentage = configuration.getSmallRequestOverheadPercentage();
        // number of seconds to upload len bytes at averageBandwidth rate plus the overhead
        long overhead = averageOverhead.longValue(SI.NANO(SI.SECOND));
        double duration = contentLength / averageBandwidth.doubleValue(Bandwidths.BYTES_PER_NANOSECOND) + overhead;
        // Small-object requests are expected to transmit their entire contents over the next second.
        if (duration == 0 || overhead / duration > overheadPercentage) {
            return Measure.valueOf(contentLength, Bandwidths.BYTES_PER_SECOND);
        }
        return Measure.valueOf(
                contentLength / duration * rateMultiplier(type),
                Bandwidths.BYTES_PER_NANOSECOND
        );
    }

    /**
     * Returns the expected bandwidth usage of a request with the given content-length in bytes per second for use by
     * the {@link TrafficController}.
     */
    static long expectedBandwidth(RequestType type, long contentLength, Configuration configuration) {
        return expectedBandwidthRate(type, contentLength, configuration).longValue(Bandwidths.BYTES_PER_SECOND);
    }

    /**
     * The API operations that a recorded request can represent.
     */
    public enum RequestType {
        PutObject,
        GetObject
    }

    /**
     * The configuration used by this traffic recorder.  This is owned by the traffic controller.
     */
    private final Configuration configuration;

    /**
     * Active requests whose bandwidth this {@link TrafficRecorder} records.
     */
    private final Map<String, RequestInfo> requests;

    /**
     * The number of active requests.  This is stored separately from {@link TrafficRecorder#requests} and as an atomic
     * integer because {@link TrafficController#acquire}, which does not run in any Verticle's thread, accesses it.
     */
    private final AtomicInteger concurrency;

    /**
     * A {@link Random} instance for traffic logging samples.
     */
    private final Random random;

    /**
     * The total bandwidth consumed by all active requests that this {@link TrafficRecorder} is recording.  In bytes per
     * nanosecond.
     */
    private volatile double bandwidth;

    /**
     * Per WebServer per Tenant usage
     */
    private final Map<String, BandwidthRecorder> tenantBandwidthUsage;

    // Gauges to track bandwidth and concurrency
    private Metrics.LongGauge bandwidthGuage;
    private Metrics.LongGauge concurrencyGuage;

    public TrafficRecorder(Configuration configuration) {
        this.configuration = configuration;
        requests = new HashMap<>();
        concurrency = new AtomicInteger(0);
        random = new Random();
        this.bandwidth = 0.0;
        tenantBandwidthUsage = new ConcurrentHashMap<>();
    }

    /**
     * Helper method for consistently logging bandwidths.
     *
     * @param bytesPerNanosecond A bandwidth in bytes per nanosecond
     *
     * @return That bandwidth in megabits per second
     */
    private double reportableBandwidth(double bytesPerNanosecond) {
        return reportableBandwidth(Measure.valueOf(bytesPerNanosecond, Bandwidths.BYTES_PER_NANOSECOND));
    }

    /**
     * Helper method for consistently logging bandwidths.
     *
     * @param bandwidth A bandwidth to log
     *
     * @return That bandwidth in megabits per second
     */
    private double reportableBandwidth(Measure<DataRate> bandwidth) {
        return bandwidth.doubleValue(Bandwidths.MEGABITS_PER_SECOND);
    }

    /**
     * Called when a request first arrives to begin tracking it.
     *
     * @param tenantNameSpace unique namespace associated with object's tenancy
     * @param rid             The request's OPC ID.
     * @param type            The request's API operation.
     * @param len             The request's content's length.
     */
    public void requestStarted(String tenantNameSpace, String rid, RequestType type, long len) {
        requestStarted(tenantNameSpace, rid, type, len, System.nanoTime());
    }

    /**
     * Like {@link TrafficRecorder#requestStarted(String, String, RequestType, long)}, except the last argument is the current
     * time in nanoseconds.
     *
     * @param rid       The request's OPC ID.
     * @param type      The request's API operation.
     * @param len       The request's content's length.
     * @param timestamp The current time.  Should be from the
     */
    public void requestStarted(String tenantNameSpace, String rid, RequestType type, long len, long timestamp) {
        Preconditions.checkState(!requests.containsKey(rid));

        final double initialRate = expectedBandwidthRate(type, len, configuration).doubleValue(
                Bandwidths.BYTES_PER_NANOSECOND
        );

        final RequestInfo req = new RequestInfo(
                tenantNameSpace,
                rid,
                type,
                timestamp,
                random.nextDouble() * 100 < configuration.getRequestSamplePercentage(),
                initialRate,
                len,
                configuration.getAlpha()
        );
        if (req.isLogEvents()) {
            trafficLog.info("start {} {} {} {} {} {}", rid, type, timestamp, len,
                    reportableBandwidth(initialRate), tenantNameSpace);
        }
        requests.put(rid, req);
        concurrency.incrementAndGet();

        // Add the new bandwidth to our reported amount immediately so that we don't oversubscribe the web server
        // when a lot of new requests arrive around the same time.
        bandwidth += initialRate;

        if (tenantNameSpace != null) {
            BandwidthRecorder tbr = tenantBandwidthUsage.computeIfAbsent(tenantNameSpace, k -> new BandwidthRecorder());
            tbr.requests++;
            tbr.bandwidth += initialRate;
        }
    }

    /**
     * Update the {@link TrafficRecorder}'s bandwidth estimate by totalling and updating the bandwidth across all
     * requests.  This is called from a periodic Vert.x timer established in
     * {@link com.oracle.pic.casper.webserver.server.WebServerVerticle#start},
     * so it is _not_ called from a separate thread.
     *
     * This is accomplished in a periodic timer and not in {@link TrafficRecorder#bufferArrived} to smooth out
     * measurement jitter.
     *
     * Should be called on equal, nonzero ticks as close to is possible, since any bytes received between ticks
     * will be decayed the same amount. If an update is called twice on the same timestamp, any bytes received
     * between those updates will not have an effect until update is called on a new timestamp.
     */
    public void update() {
        update(System.nanoTime());
    }

    /**
     * Like Like {@link TrafficRecorder#update(), except it accepts the current timestamp as an argument.
     *
     * @param time time in nanoseconds.
     */
    public void update(long time) {
        BandwidthRecorder br = new BandwidthRecorder();
        br.beginEstimate();

        for (BandwidthRecorder tenantBandwidthRecorder : tenantBandwidthUsage.values()) {
            tenantBandwidthRecorder.beginEstimate();
        }

        if (!requests.isEmpty()) {
            for (RequestInfo req : requests.values()) {
                boolean isUpdated = req.isUpdated();

                req.ewma(time, rateMultiplier(req.getType()));

                double rateEst = req.getEstimatedRate();
                if (isUpdated && req.isLogEvents()) {
                    trafficLog.info("update {} {} {} {} {}",
                            req.getID(),
                            req.getBytesReceived(),
                            time,
                            reportableBandwidth(rateEst),
                            req.getTenantNameSpace()
                    );
                }

                // Update global estimate
                br.updateEstimate(rateEst);

                // Update tenant level estimate as well
                if (req.tenantNameSpace != null) {
                    tenantBandwidthUsage.get(req.tenantNameSpace).updateEstimate(rateEst);
                }
            }
        }

        br.endEstimate();
        for (BandwidthRecorder tenantBandwidthRecorder : tenantBandwidthUsage.values()) {
            tenantBandwidthRecorder.endEstimate();
        }

        bandwidth = br.bandwidth;

        // Publish Metrics
        Optional.ofNullable(bandwidthGuage).ifPresent(b -> b.set(getBandwidthUsage()));
        Optional.ofNullable(concurrencyGuage).ifPresent(c -> c.set(getConcurrency()));
        publishWhitelistedMetrics();
    }

    private void publishWhitelistedMetrics() {
        if (Vertx.currentContext() != null) {
            Set<String> wlns = configuration.getMetricWhitelistedNamespaces();
            if (wlns != null && wlns.size() > 0) {
                for (String ns : wlns) {
                    long bw = getTenantBandwidthUsage(ns);
                    long concurrency = getTenantConcurrency(ns);
                    Map<String, String> dimensions = ImmutableMap.of("thread", Thread.currentThread().getName(),
                            "tenantNameSpace", ns);
                    Metrics.gauge(MetricName.of("ws.tc.recorder.tenant.bandwidth", dimensions)).set(bw);
                    Metrics.gauge(MetricName.of("ws.tc.recorder.tenant.concurrency", dimensions))
                            .set(concurrency);
                }
            }
        }
    }

    /**
     * Record the arrival of request data.
     *
     * @param rid The request's OPC ID.
     * @param len The amount of arrived data - really, the length of the buffer delivered by Vert.x.
     */
    public void bufferArrived(String rid, long len) {
        bufferArrived(rid, len, System.nanoTime());
    }


    /**
     * Same as bufferArrived above
     *
     * @param rid  The request's OPC ID.
     * @param len  The amount of arrived data - really, the length of the buffer delivered by Vert.x.
     * @param time The time in nanoseconds at which the data arrived.
     */
    public void bufferArrived(String rid, long len, long time) {
        if (requests.containsKey(rid)) {
            final RequestInfo req = requests.get(rid);
            req.addBytesReceived(len);
        }
    }

    /**
     * Remove a request that has ended and thus will consume no more bandwidth from the recorder.
     *
     * @param rid The request's OPC ID.
     */
    public void requestEnded(String rid) {
        requestEnded(rid, System.nanoTime());
    }

    /**
     * Same as requestEnded above
     *
     * @param rid       The request's OPC ID.
     * @param timestamp The time in nanoseconds at which the request ended
     */
    public void requestEnded(String rid, long timestamp) {
        if (requests.containsKey(rid)) {
            final RequestInfo req = requests.remove(rid);
            concurrency.decrementAndGet();

            final String tenantNameSpace = req.getTenantNameSpace();

            // This would reset tenant limits immediately though overall aggregation still has memory associated with it
            if (tenantNameSpace != null) {
                Preconditions.checkState(tenantBandwidthUsage.containsKey(tenantNameSpace));

                BandwidthRecorder tbr = tenantBandwidthUsage.get(tenantNameSpace);
                if (--tbr.requests < 1) {
                    tenantBandwidthUsage.remove(tenantNameSpace);
                }
            }

            if (req.isLogEvents()) {
                trafficLog.info("end {} {} {} {}",
                        rid,
                        timestamp,
                        reportableBandwidth(req.getEstimatedRate()),
                        tenantNameSpace);
            }
        }
    }

    /**
     * Return the {@link TrafficRecorder}'s estimated bandwidth.  See {@link TrafficRecorder#update(long)} and {@link
     * TrafficRecorder#bufferArrived(String, long)}.
     *
     * @return The estimated bandwidth in bytes per second for use by the {@link TrafficController}.
     */
    public long getBandwidthUsage() {
        return Measure.valueOf(bandwidth, Bandwidths.BYTES_PER_NANOSECOND).longValue(Bandwidths.BYTES_PER_SECOND);
    }

    public long getTenantBandwidthUsage(String tenantNameSpace) {
        if (tenantNameSpace == null || !tenantBandwidthUsage.containsKey(tenantNameSpace)) {
            return 0L;
        }

        BandwidthRecorder tbr = tenantBandwidthUsage.get(tenantNameSpace);
        return Measure.valueOf(tbr.bandwidth, Bandwidths.BYTES_PER_NANOSECOND).longValue(Bandwidths.BYTES_PER_SECOND);
    }

    public long getTenantConcurrency(String tenantNameSpace) {
        if (tenantNameSpace == null || !tenantBandwidthUsage.containsKey(tenantNameSpace)) {
            return 0L;
        }

        BandwidthRecorder tbr = tenantBandwidthUsage.get(tenantNameSpace);
        return tbr.requests;
    }

    /**
     * Return the request's individual estimated bandwidth.
     *
     * @param id id of the request
     *
     * @return The estimated bandwidth in Megabits per second used by the request of name id
     */
    public double getReqBandwidthUsage(String id) {
        return reportableBandwidth(requests.get(id).getEstimatedRate());
    }

    /**
     * Return the number of active requests this {@link TrafficRecorder} is recording.
     *
     * @return The number of active requests.
     */
    public int getConcurrency() {
        return concurrency.get();
    }


    /**
     * A request from the perspective of a {@link TrafficRecorder}.
     */
    private static final class RequestInfo {
        private final String rid;
        private final RequestType type;

        private final String tenantNameSpace;

        /**
         * The time the request started in nanoseconds.
         */
        private final long startTime;

        /**
         * The time the last EWMA was calculated in nanoseconds (begins equal to start time)
         */
        private long lastTime;

        /**
         * The number of bytes received for the request.
         */
        private long bytesReceived;

        /**
         * The number of bytes received since the last update for the request.
         */
        private long newBytesReceived;

        /**
         * The initial estimate for the rate at which data is arriving.
         */
        private final double rateEstInit;

        /**
         * The estimated rate at which data is arriving in bytes.
         */
        private double rateEst;

        /**
         * The content length of the request.
         */
        private final long contentLen;

        /**
         * A number that determines how quickly the moving average changes. A higher alpha means that weight will be
         * placed more on the most recent updates. Should be within (0,1], where 1 indicates only using data from the
         * most recent update.
         */
        private final double alpha;

        /**
         * True if the request has received a buffer since the last time update() was called. This is used to avoid
         * redundant log messages for requests that have not been updated.
         */
        private boolean updated;

        /**
         * True if the events for this request will be added to the traffic log. The traffic log is sampled, so only a
         * small percentage of requests are logged. If any events for a request are logged, all events for that request
         * are logged.
         */
        private final boolean logEvents;

        RequestInfo(
                String tenantNameSpace,
                String rid, RequestType type,
                long startTime,
                boolean logEvents,
                double initialEst,
                long contentLen,
                double alpha
        ) {
            this.rid = rid;
            this.type = type;
            this.tenantNameSpace = tenantNameSpace;
            this.startTime = startTime;
            this.lastTime = startTime;
            this.bytesReceived = 0;
            this.newBytesReceived = 0;
            this.updated = false;
            this.logEvents = logEvents;
            this.rateEstInit = initialEst;
            this.rateEst = initialEst;
            this.contentLen = contentLen;
            this.alpha = alpha;
        }

        public String getID() {
            return rid;
        }

        public RequestType getType() {
            return type;
        }

        public long getBytesReceived() {
            return bytesReceived;
        }

        public String getTenantNameSpace() {
            return tenantNameSpace;
        }

        public void addBytesReceived(long bytes) {
            newBytesReceived += bytes;
            bytesReceived += bytes;
            updated = true;
        }

        public boolean isUpdated() {
            return updated;
        }

        public boolean isLogEvents() {
            return logEvents;
        }

        public double getEstimatedRate() {
            return rateEst;
        }

        /**
         * Updates the rate estimate with the newest amount of bytes received. For any new update (i.e., the timestamp
         * is different than the last timestamp), uses the formula
         *
         * rateEstimate = alpha*(rate since last timestamp) + (1-alpha)*oldRateEstimate.
         *
         * Higher alpha values will therefore quicken the decay of older rates.
         *
         * @param timestamp time in nanoseconds at which to perform the update
         */
        public void ewma(long timestamp, long rateMultiplier) {
            final double dt = timestamp - lastTime;
            if (dt > 0 && contentLen > 0) {
                double rateObs = rateMultiplier * newBytesReceived / dt;
                rateEst = alpha * rateObs + (1.0 - alpha) * rateEst;
                updated = false;
                newBytesReceived = 0;
                lastTime = timestamp;
            }
        }

    }


    /**
     * The bandwidth consumed by a set of requests.  Its estimate {@link BandwidthRecorder#sum} is updated
     * incrementally and then atomically made visible to readers as {@link BandwidthRecorder#bandwidth}; consequently
     * this value can be safely read from multiple threads.
     */
    private static final class BandwidthRecorder {
        private double c = 0.0;
        private double sum = 0.0;
        private volatile double bandwidth = 0.0;

        /**
         * How many requests recorded so far.  When this number drops to zero, the state is garbage and shouldnt be
         * further used, without resetting via beginEstimate
         */
        private long requests = 0;

        /**
         * Begin recalculating the estimate of this tenant's bandwidth usage.  Call this at the beginning of
         * {@link TrafficRecorder#update(long)}
         */
        private void beginEstimate() {
            c = 0.0;
            sum = 0.0;
            requests = 0;
        }

        /**
         * Update bandwidth estimate with a request's estimated rate.  Because each rate estimate is for
         * one request, this also increments {@link BandwidthRecorder#requests} by one.
         *
         * @param rateEst The estimated rate of a request
         */
        private void updateEstimate(double rateEst) {
            // Minimize accumulated error with Kahan summation:
            // https://en.wikipedia.org/wiki/Kahan_summation_algorithm#The_algorithm
            final double y = rateEst - c;
            final double t = sum + y;
            c = (t - sum) - y;
            sum = t;

            requests++;
        }

        /**
         * Finish the bandwidth estimate calculation and make it visible to readers as
         * {@link BandwidthRecorder#bandwidth}.
         */
        private void endEstimate() {
            bandwidth = sum;
        }
    }
}
