package com.oracle.pic.casper.webserver.api.ratelimit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.metrics.Metrics;

/**
 * A collection of metrics to track embargo rule evaluation. There are three
 * pieces of information collected here:
 *  1) How many requests were evaluated against a rule.
 *  2) How many requests matched the rule.
 *  3) How many requests were blocked because of the rule.
 * In addition we calculate percentages for #2 and #3 and publish those.
 */
public class EmbargoV3MetricsBundle {

    private static final String REQUESTS_SUFFIX = ".requests";
    private static final String MATCHED_SUFFIX = ".matched";
    private static final String BLOCKED_SUFFIX = ".blocked";
    private static final String MATCHRATE_SUFFIX = ".matchrate";
    private static final String BLOCKRATE_SUFFIX = ".blockrate";

    /**
     * The prefix used to create the metrics.
     */
    private final String prefix;

    /**
     * Number of requests that were evaluated against the rule.
     */
    private final Counter requests;

    /**
     * Number of requests that matched the rule.
     */
    private final Counter matched;

    /**
     * Number of requests that were blocked by the rule.
     */
    private final Counter blocked;

    /**
     * Percentage of requests that matched the rule. This is {@link #matched}
     * divided by {@link #requests}.
     */
    private final Histogram matchedPercentage;

    /**
     * Percentage of requests that were blocked by the rule. This is {@link #blocked}
     * divided by {@link #requests}.
     */
    private final Histogram blockedPercentage;

    public EmbargoV3MetricsBundle(String prefix) {
        this.prefix = Preconditions.checkNotNull(prefix);
        Preconditions.checkState(this.prefix.length() > 0);

        this.requests = Metrics.counter(prefix + REQUESTS_SUFFIX);
        this.matched = Metrics.counter(prefix + MATCHED_SUFFIX);
        this.blocked = Metrics.counter(prefix + BLOCKED_SUFFIX);

        this.matchedPercentage = Metrics.percentageHistogram(prefix + MATCHRATE_SUFFIX, matched, requests);
        this.blockedPercentage = Metrics.percentageHistogram(prefix + BLOCKRATE_SUFFIX, blocked, requests);
    }

    /**
     * Gets the prefix this bundle uses.
     */
    public String getPrefix() {
        return this.prefix;
    }

    /**
     * Called whenever the rule is evaluated against an operation. If the rule
     * matches the operation {@link #ruleMatched()} should be called, and if the
     * rule then blocks the operation {@link #operationBlocked()} should be called.
     */
    public void evaluatedRule() {
        this.requests.inc();
    }

    /**
     * Called whenever the rule matches an operation. {@link #evaluatedRule()} should
     * be called before this method.
     */
    public void ruleMatched() {
        this.matched.inc();
    }

    /**
     * Called whenever the rule blocked an operation. {@link #evaluatedRule()} and
     * {@link #ruleMatched()} should be called before this method.
     */
    public void operationBlocked() {
        this.blocked.inc();
    }

    public Counter getRequests() {
        return requests;
    }

    public Counter getMatched() {
        return matched;
    }

    public Counter getBlocked() {
        return blocked;
    }
}
