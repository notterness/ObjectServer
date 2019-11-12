package com.oracle.pic.casper.webserver.api.usage;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.PercentageHistogram;

public final class QuotaEvaluationMetrics {
    private QuotaEvaluationMetrics() {
    }

    /**
     * Records the number of requests that exceeded storage limit
     */
    public static final Meter LIMIT_EVALUATION_REQUEST_FAILED = Metrics.meter("ws.usage.evaluation.limit.failed");

    /**
     * Records the number of requests that exceeded compartment quota
     */
    public static final Meter QUOTA_EVALUATION_REQUEST_FAILED = Metrics.meter("ws.usage.evaluation.quota.failed");

    /**
     * Records the number of requests that were skipped during quota evaluation due to no limit or quota policy
     */
    public static final Meter EVALUATION_REQUEST_SKIPPED = Metrics.meter("ws.usage.evaluation.request.skipped");

    /**
     * Records the number of requests that passed quota evaluation (includes skipped requests)
     */
    public static final Meter EVALUATION_REQUEST_PASSED = Metrics.meter("ws.usage.evaluation.request.passed");

    /**
     * Records quota evaluation latency
     */
    public static final Histogram EVALUATION_LATENCY = Metrics.unaggregatedHistogram("ws.usage.evaluation.latency");

    /**
     * Records the number of requests rejected since no more threads available in the pool
     */
    public static final Meter EVALUATION_REJECTED = Metrics.meter("ws.usage.evaluation.rejected");

    /**
     * Records the number of requests bailed out during quota evaluation due to timeout
     */
    public static final Meter EVALUATION_TIMEOUTS = Metrics.meter("ws.usage.evaluation.timeout");

    /**
     * Records the number of sdk errors during quota evaluation
     */
    public static final Meter QUOTA_ERRORS = Metrics.meter("ws.usage.evaluation.quota.errors");

    /**
     * Records the number of runtime errors during quota evaluation
     */
    public static final Meter UNKNOWN_ERRORS = Metrics.meter("ws.usage.evaluation.unknown.errors");

    /**
     * Records the total number of requests went through quota evaluation
     */
    public static final Meter EVALUATION_REQUESTS_TOTAL = Metrics.meter("ws.usage.evaluation.requests.total");

    /**
     * Records the number of observed errors during quota evaluation
     */
    public static final Meter EVALUATION_ERRORS_TOTAL = Metrics.meter("ws.usage.evaluation.errors.total");

    /**
     * Tracks the percentage of errors during quota evaluation
     */
    public static final PercentageHistogram EVALUATION_ERRORS_PERCENTAGE = Metrics.percentageHistogram(
            "ws.usage.evaluation.errors.percentage",
            EVALUATION_ERRORS_TOTAL,
            EVALUATION_REQUESTS_TOTAL
    );
}
