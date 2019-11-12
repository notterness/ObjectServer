package com.oracle.pic.casper.webserver.api.ratelimit;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.RateLimiter;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class EmbargoV3Impl implements EmbargoV3 {

    private static final Logger LOG = LoggerFactory.getLogger(Backend.class);

    /**
     * Used to read the embargo rules. This gets the rules from the operator
     * database and handles caching and retries.
     */
    private final EmbargoRuleCollector ruleCollector;

    /**
     * A collection of rate limiters for the rules. Using a cache means we
     * do not have to worry about handling disabled rules -- they will
     * naturally drop out of the cache.
     */
    private final Cache<Long, RateLimiter> rateLimiters;

    EmbargoV3Impl(EmbargoRuleCollector ruleCollector, Ticker ticker) {
        this.ruleCollector = ruleCollector;
        this.rateLimiters = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .ticker(ticker)
            .build();
    }

    public EmbargoV3Impl(EmbargoRuleCollector ruleCollector) {
        this(ruleCollector, Ticker.systemTicker());
    }

    @Override
    public void enter(EmbargoV3Operation operation) {
        try {
            // If there are no rules configured, then embargo doesn't kick in
            if (ruleCollector.getDryRunRules().size() == 0 && ruleCollector.getEnabledRules().size() == 0) {
                return;
            }

            // We look at the enabled rules first. Why? We want the metrics for
            // a dry-run rule to only describe operations that would be blocked
            // if the rule was enabled. If an operation matches both an enabled
            // rule and a dry-run rule we do not want to double-count.
            final EmbargoV3Rule blockingRule = evaluateEnabledRules(operation);
            if (blockingRule != null) {
                final String message;
                if (blockingRule.getAllowedRate() == 0.0) {
                    message = String.format("operation blocked [0x%x]", blockingRule.getId());
                } else {
                    message = String.format("rate-limit exceeded [0x%x]", blockingRule.getId());
                }
                throw new EmbargoException(message);
            }

            // None of the enabled rules blocked an operation. Look at the
            // dry-run rules.
            evaluateDryRunRules(operation);
        } catch (ExecutionException e) {
            LOG.error("Got ExecutionException in EmbargoV3Impl::enter", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Evaluate enabled rules. If a rule blocks the operation the matching
     * rule is returned.
     */
    private EmbargoV3Rule evaluateEnabledRules(EmbargoV3Operation operation) throws ExecutionException {
        if (ruleCollector.getEnabledRules().size() == 0) {
            return null;
        }
        WebServerMetrics.EMBARGOV3_ENABLED.evaluatedRule();

        // Points to the rule that will block the operation
        EmbargoV3Rule blockingRule = null;

        // True if at least one rule matched the operation. Note that a rule
        // can match an operation without blocking the operation.
        boolean atLeastOneMatch = false;
        for (EmbargoV3Rule rule : ruleCollector.getEnabledRules()) {
            final EmbargoV3MetricsBundle ruleMetrics = getMetricsBundle(
                WebServerMetrics.EMBARGOV3_ENABLED.getPrefix(),
                rule,
                WebServerMetrics.EMBARGOV3_ENABLED_RULES);
            ruleMetrics.evaluatedRule();
            if (rule.matches(operation)) {
                ruleMetrics.ruleMatched();
                atLeastOneMatch = true;
                if (operationIsAllowed(rule)) {
                    LOG.debug("Request {} matches embargo rule {}. The operation is allowed.",
                        operation,
                        rule.getId());
                } else if (blockingRule == null) {
                    ruleMetrics.operationBlocked();
                    blockingRule = rule;
                    LOG.warn("Request {} matches embargo rule {}. The operation is blocked.",
                        operation,
                        rule.getId());
                }
            }
        }

        if (atLeastOneMatch) {
            WebServerMetrics.EMBARGOV3_ENABLED.ruleMatched();
        }

        if (blockingRule != null) {
            assert atLeastOneMatch;
            WebServerMetrics.EMBARGOV3_ENABLED.operationBlocked();
        }

        return blockingRule;
    }

    /**
     * Evaluate dry-run rules. If a rule blocks the operation the matching
     * rule is returned.
     */
    private EmbargoV3Rule evaluateDryRunRules(EmbargoV3Operation operation) throws ExecutionException {
        if (ruleCollector.getDryRunRules().size() == 0) {
            return null;
        }

        WebServerMetrics.EMBARGOV3_DRYRUN.evaluatedRule();

        // Points to the rule that would block the operation
        EmbargoV3Rule blockingRule = null;

        // True if at least one rule matched the operation. Note that a rule
        // can match an operation without blocking the operation.
        boolean atLeastOneMatch = false;
        for (EmbargoV3Rule rule : ruleCollector.getDryRunRules()) {
            final EmbargoV3MetricsBundle ruleMetrics = getMetricsBundle(
                WebServerMetrics.EMBARGOV3_DRYRUN.getPrefix(),
                rule,
                WebServerMetrics.EMBARGOV3_DRYRUN_RULES);
            ruleMetrics.evaluatedRule();
            if (rule.matches(operation)) {
                ruleMetrics.ruleMatched();
                atLeastOneMatch = true;
                if (operationIsAllowed(rule)) {
                    LOG.debug("Request {} matches dry-run embargo rule {}. The operation would be allowed.",
                        operation,
                        rule.getId());
                } else if (blockingRule == null) {
                    ruleMetrics.operationBlocked();
                    blockingRule = rule;
                    LOG.debug("Request {} matches dry-run embargo rule {}. The operation would be blocked.",
                        operation,
                        rule.getId());
                }
            }
        }

        if (atLeastOneMatch) {
            WebServerMetrics.EMBARGOV3_DRYRUN.ruleMatched();
        }

        if (blockingRule != null) {
            assert atLeastOneMatch;
            WebServerMetrics.EMBARGOV3_DRYRUN.operationBlocked();
        }

        return blockingRule;
    }

    /**
     * Given a rule that has matched an operation determine if the operation
     * should be allowed.
     */
    private boolean operationIsAllowed(EmbargoV3Rule matchedRule) throws ExecutionException {
        if (matchedRule.getAllowedRate() == 0.0) {
            // Rate of 0 means the operation is never allowed
            return false;
        }
        return rateLimiterForRule(matchedRule).tryAcquire();
    }

    /**
     * Find the cached {@link RateLimiter} for the rule, creating it if
     * necessary.
     */
    private RateLimiter rateLimiterForRule(EmbargoV3Rule rule) throws ExecutionException {
        final long id = rule.getId();
        final double operationsPerSecond = rule.getAllowedRate();
        return rateLimiters.get(id, () -> RateLimiter.create(operationsPerSecond));
    }

    /**
     * Get the metrics bundle for the rule from the hash table. If no metrics
     * bundle exist, create one.
     */
    private EmbargoV3MetricsBundle getMetricsBundle(
            String prefix,
            EmbargoV3Rule rule,
            Map<Long, EmbargoV3MetricsBundle> bundles) {
        final long id = rule.getId();
        return bundles.computeIfAbsent(id, x -> new EmbargoV3MetricsBundle(prefix + "." + Long.toString(id)));
    }
}
