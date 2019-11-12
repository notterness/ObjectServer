package com.oracle.pic.casper.webserver.limit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.PercentageHistogram;

public class ResourceControlMetricsBundle {

    /**
     * A counter on the total number of acquires to a resource.
     */
    private Counter requests;

    /**
     * A counter on the total number of successful acquire to a resource.
     */
    private Counter success;

    /**
     * A counter on the total number of failed acquire to a resource due to resource control.
     */
    private Counter failure;

    /**
     * Tracks latency of calls to acquire a resource.
     */
    private Histogram latency;

    /**
     * Tracks the percentage of failure to acquire a resource due to resource control.
     */
    private PercentageHistogram failurePercentage;

    public ResourceControlMetricsBundle(String name) {
        requests = Metrics.counter("ws.resource." + name + ".requests");
        success = Metrics.counter("ws.resource." + name + ".success");
        failure = Metrics.counter("ws.resource." + name + ".failure");
        latency = Metrics.unaggregatedHistogram("ws.resource." + name + ".latency");
        failurePercentage = Metrics.percentageHistogram(
                "ws.resource." + name + ".failure.percent", failure, requests);
    }

    public Counter getRequests() {
        return requests;
    }

    public Counter getSuccess() {
        return success;
    }

    public Counter getFailure() {
        return failure;
    }

    public Histogram getLatency() {
        return latency;
    }

    public PercentageHistogram getFailurePercentage() {
        return failurePercentage;
    }
}
