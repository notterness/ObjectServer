package com.oracle.pic.casper.webserver.traffic;

import com.codahale.metrics.Counter;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;

import javax.measure.Measure;
import javax.measure.quantity.DataRate;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.oracle.pic.casper.webserver.traffic.Bandwidths.BYTES_PER_SECOND;

/**
 * TrafficController enforces a traffic contract for requests to the web server.
 *
 * The traffic contract currently enforced by the controller is very simple and consists of the following:
 *
 * - The TrafficController is configured with a "maximum bandwidth" for the web server.
 *
 * - The TrafficController uses TrafficRecorders to estimate the current bandwidth usage of the server.
 *
 * - The TrafficController has a simple model for estimating the future bandwidth usage of GET and PUT object requests.
 *
 * - When a new request arrives, the current bandwidth usage is estimated and combined with the future estimate for the
 * new request.  If that total is greater than the maximum bandwidth for the web server, the request is rejected,
 * otherwise it is accepted.
 * It also leverages the tenant namespace level bandwidth usage, and throttles tenants that are consuming a fixed
 * percentage of bandwidth.
 *
 * The TrafficController emits the following metrics:
 *
 * - ws.traffic.bandwidth: the last estimate of the bandwidth used by the web server, this is only updated when a new
 * request arrives.
 *
 * - ws.traffic.concurrency: the number of PUT and GET object requests currently in progress.
 *
 * - ws.traffic.get.accept: a counter for the number of accepted GET requests.
 *
 * - ws.traffic.get.reject: a counter for the number of rejected GET requests.
 *
 * - ws.traffic.put.accept: a counter for the number of accepted PUT requests.
 *
 * - ws.traffic.put.reject: a counter for the number of rejected PUT requests.
 *
 * TrafficController instances are thread safe.
 */
public final class TrafficController {

    private final List<TrafficRecorder> recorders;
    private volatile boolean shadowMode;
    private volatile Measure<DataRate> maxBandwidth;
    private volatile TrafficRecorder.Configuration trafficRecorderConfiguration;
    private final TenantThrottler tenantThrottler;

    public TrafficController(boolean shadowMode,
                             boolean tenantThrottleShadowMode,
                             Measure<DataRate> maxBandwidth,
                             TrafficRecorder.Configuration trafficRecorderConfiguration) {
        this.shadowMode = shadowMode;
        this.maxBandwidth = maxBandwidth;
        this.trafficRecorderConfiguration = trafficRecorderConfiguration;
        this.recorders = new CopyOnWriteArrayList<>();
        tenantThrottler = new TenantThrottler(tenantThrottleShadowMode);
    }

    public void addRecorder(TrafficRecorder recorder) {
        recorders.add(recorder);
    }

    public void setShadowMode(boolean shadowMode) {
        this.shadowMode = shadowMode;
    }

    public boolean isShadowMode() {
        return this.shadowMode;
    }

    public Measure<DataRate> getMaxBandwidth() {
        return maxBandwidth;
    }

    public void setMaxBandwidth(Measure<DataRate> maxBandwidth) {
        this.maxBandwidth = maxBandwidth;
    }

    public TrafficRecorder.Configuration getTrafficRecorderConfiguration() {
        return trafficRecorderConfiguration;
    }

    public void acquire(String tenantNameSpace, TrafficRecorder.RequestType type, long len) {
        long totalbw = 0;
        long tenantbw = 0;
        int tcount = 0;
        boolean isAllowed;

        total(type).inc();

        for (TrafficRecorder recorder : recorders) {
            totalbw += recorder.getBandwidthUsage();
            tenantbw += recorder.getTenantBandwidthUsage(tenantNameSpace);
            tcount += recorder.getConcurrency();
        }

        WebServerMetrics.TC_CONCURRENCY.set(tcount);

        final long ebw = TrafficRecorder.expectedBandwidth(type, len, trafficRecorderConfiguration);
        final long mbw = maxBandwidth.longValue(BYTES_PER_SECOND);
        if (totalbw + ebw > mbw) {
            WebServerMetrics.TC_BANDWIDTH.set(totalbw);
            decision(type, shadowMode).inc();
            isAllowed = shadowMode;
        } else {
            WebServerMetrics.TC_BANDWIDTH.set(totalbw + ebw);
            decision(type, true).inc();
            isAllowed = true;
        }

        if (!isAllowed) {
            throw new TrafficControlException("the service is currently unavailable");
        }

        if (!tenantThrottler.shouldAcceptRequest(tenantNameSpace,
                type,
                tenantbw + ebw,
                mbw)) {
            throw new TenantThrottleException("Too many requests!");
        }
    }

    private Counter total(TrafficRecorder.RequestType type) {
        switch (type) {
            case GetObject:
                return WebServerMetrics.TC_GET_TOTAL;
            case PutObject:
                return WebServerMetrics.TC_PUT_TOTAL;
            default:
                throw new RuntimeException(String.format("Unknown request type %s", type));
        }
    }

    private Counter decision(TrafficRecorder.RequestType type, boolean accept) {
        switch (type) {
            case GetObject:
                return accept ? WebServerMetrics.TC_GET_ACCEPT : WebServerMetrics.TC_GET_REJECT;
            case PutObject:
                return accept ? WebServerMetrics.TC_PUT_ACCEPT : WebServerMetrics.TC_PUT_REJECT;
            default:
                throw new RuntimeException(String.format("Unknown request type %s", type));
        }
    }

    public TenantThrottler getTenantThrottler() {
        return tenantThrottler;
    }
}
