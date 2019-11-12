package com.oracle.pic.casper.webserver.limit;

import com.codahale.metrics.Counter;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.PercentageHistogram;

/**
 * A collection of metrics that is used by the web server service to help disseminate information
 * about the running process.
 */
public final class ResourceControlMetrics {

    /**
     * Constructor which is not to be called
     */
    private ResourceControlMetrics() {
    }

    /**
     * The metrics bundle for identity resource acquisition.
     */
    static final ResourceControlMetricsBundle RESOURCE_IDENTITY = new ResourceControlMetricsBundle("identity");

    /**
     * A counter on the total number of calls to acquire resource by a tenant.
     */
    static final Counter RESOURCE_TENANT_REQUEST = Metrics.counter("ws.resource.tenant.request");

    /**
     * A counter on the total number of successful calls to acquire resource by a tenant.
     */
    static final Counter RESOURCE_TENANT_SUCCESS = Metrics.counter("ws.resource.tenant.success");

    /**
     * A counter on the total number of failed calls to acquire resource due to per tenant resource control.
     */
    static final Counter RESOURCE_TENANT_FAILURE = Metrics.counter("ws.resource.tenant.failure");

    /**
     * Tracks the percentage of failure to acquire a resource due to per tenant resource control.
     */
    static final PercentageHistogram RESOURCE_TENANT_FAILURE_PERCENTAGE = Metrics.percentageHistogram(
            "ws.resource.tenant.failure.percent", RESOURCE_TENANT_FAILURE, RESOURCE_TENANT_REQUEST);
}
