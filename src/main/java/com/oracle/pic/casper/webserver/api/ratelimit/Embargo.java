package com.oracle.pic.casper.webserver.api.ratelimit;

import com.google.common.collect.Lists;
import com.oracle.pic.casper.common.metrics.MetricScope;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.util.CommonRequestContext;
import com.oracle.pic.casper.mds.operator.MdsEmbargoRuleV1;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;

/**
 * This is the customs & border protection class for Casper - all requests must go through this class to be issued
 * an {@link EmbargoContext} (a.k.a visa) which they must present upon leaving.
 * This class can deny requests based on a variety of factors
 * such as method, uri, and {@link CasperOperation}
 */
@Deprecated
public class Embargo {

    private static Logger logger = LoggerFactory.getLogger(Embargo.class);

    private final EmbargoRuleCollector embargoRuleCollector;

    public Embargo(EmbargoRuleCollector embargoRuleCollector) {
        this.embargoRuleCollector = embargoRuleCollector;
    }

    /**
     * Enter the Embargo. This is done as early as possible before authentication.
     * This method will increment the global conccurrent requests counter
     *
     * @param context The current request contxt
     * @param method  The current http method
     * @param uri     The current URI
     * @return A visa that is used for the other operations in the Embargo
     * @throws EmbargoException if the max global requests exceeds the set value
     */
    public EmbargoContext enter(CommonRequestContext context, String method, String uri,
                                String userAgent) {
        logger.trace("Entering the embargo with method {} and uri {}", method, uri);

        final Collection<MdsEmbargoRuleV1> embargoRules = embargoRuleCollector.getEmbargoRules();

        checkForEmbargoRuleMatch(context, null, null, method, uri, userAgent, embargoRules);
        return new EmbargoContext(method, uri, userAgent);
    }

    /**
     * Enter the Embargo. This is done as early as possible before authentication.
     * This method will increment the global conccurrent requests counter
     *
     * @param method  The current http method
     * @param uri     The current URI
     * @return A visa that is used for the other operations in the Embargo
     * @throws EmbargoException if the max global requests exceeds the set value
     */
    public EmbargoContext enter(String method, String uri, String userAgent, MetricScope metricScope) {
        logger.trace("Entering the embargo with method {} and uri {}", method, uri);

        final Collection<MdsEmbargoRuleV1> embargoRules = embargoRuleCollector.getEmbargoRules();

        checkForEmbargoRuleMatch(metricScope, null, null, method, uri, userAgent, embargoRules);
        return new EmbargoContext(method, uri, userAgent);
    }

    /**
     * Perform customs on an authenticated user.
     * This will increment the current user
     *
     * @param visa               The previous context returned from {@link #enter}
     * @param authenticationInfo The current user entering
     * @throws EmbargoException if the current users max conn is higher than a set limit or
     *                          there are any embargo rules placed
     */
    public void customs(CommonRequestContext context,
                        EmbargoContext visa,
                        AuthenticationInfo authenticationInfo,
                        CasperOperation operation) {

        if (visa.hasEnteredCustoms()) {
            if (!visa.allowReEntry()) {
                logger.error("Embargo re-entry not allowed for visa {}, authInfo {} operation {}",
                        visa, authenticationInfo, operation);
                WebServerMetrics.EMBARGOED_REQUESTS.inc();
                throw new EmbargoException("Embargo re-entry not allowed");
            }
            return; // already entered customs will not embargo again
        } else {
            visa.enterCustoms();
        }

        final Collection<MdsEmbargoRuleV1> embargoRules = embargoRuleCollector.getEmbargoRules();
        final String tenantId = authenticationInfo.getMainPrincipal().getTenantId();
        checkForEmbargoRuleMatch(context, operation.getSummary(), tenantId,
                visa.getHttpMethod(), visa.getUri(), visa.getUserAgent(), embargoRules);

        visa.setPrincipal(authenticationInfo.getMainPrincipal()).setOperation(operation);
    }

    /**
     * Perform customs on an authenticated user.
     * This will increment the current user
     *
     * @param visa               The previous context returned from {@link #enter}
     * @param authenticationInfo The current user entering
     * @throws EmbargoException if the current users max conn is higher than a set limit or
     *                          there are any embargo rules placed
     */
    public void customs(MetricScope metricScope,
                        EmbargoContext visa,
                        AuthenticationInfo authenticationInfo,
                        CasperOperation operation) {

        if (visa.hasEnteredCustoms()) {
            if (!visa.allowReEntry()) {
                logger.error("Embargo re-entry not allowed for visa {}, authInfo {} operation {}",
                        visa, authenticationInfo, operation);
                WebServerMetrics.EMBARGOED_REQUESTS.inc();
                throw new EmbargoException("Embargo re-entry not allowed");
            }
            return; // already entered customs will not embargo again
        } else {
            visa.enterCustoms();
        }

        final Collection<MdsEmbargoRuleV1> embargoRules = embargoRuleCollector.getEmbargoRules();
        final String tenantId = authenticationInfo.getMainPrincipal().getTenantId();
        checkForEmbargoRuleMatch(metricScope, operation.getSummary(), tenantId,
                visa.getHttpMethod(), visa.getUri(), visa.getUserAgent(), embargoRules);

        visa.setPrincipal(authenticationInfo.getMainPrincipal()).setOperation(operation);
    }

    /**
     * Enter the state of Casper which decrements the global counter and the tenant specific counter if we have
     * gotten that far through the EmbargoContext.
     * You can call this function multiple times and it is idempotent
     *
     * @param visa
     */
    public void exit(EmbargoContext visa) {
        logger.trace("Exiting embargo with visa {}", visa);

        if (visa.hasExited()) {
            logger.debug("We are trying to re-close an embargo context");
            return;
        }

        visa.exit();
    }

    public EmbargoRuleCollector getRuleCollector() {
        return embargoRuleCollector;
    }

    /**
     * This method raises an EmbargoException if the request matches any of the rules in embargoRules
     *
     * The method also, if the request is going to be embargoed:
     * 1) Creates a MetricScope annotation containing all the ids that the request matched
     * 2) Increments a Metrics.Counter for each rule matched, named "ws.embargo.[RULE-ID].match"
     * 3) Increments a Metrics.Counter that reflects the total number of requests embargoed
     *
     * @param context      The CommonRequestContext for the request
     * @param operation    The Operation being performed by the request (Can be null prior to Auth)
     * @param tenantId     The tenantId of the requests Principal (can be null prior to Auth)
     * @param method       The HttpMethod of the request
     * @param uri          The uri for the request
     * @param userAgent    The userAgent for the request
     * @param embargoRules The list of embargo rules retrieved from the database
     */
    private void checkForEmbargoRuleMatch(
            CommonRequestContext context, @Nullable String operation, @Nullable String tenantId, String method,
            String uri, String userAgent, Collection<MdsEmbargoRuleV1> embargoRules) {

        final List<Long> matchedRules = Lists.newArrayList();
        for (MdsEmbargoRuleV1 embargoRule : embargoRules) {
            if (EmbargoMatcher.matches(embargoRule, operation, tenantId, uri, method, userAgent)) {
                matchedRules.add(embargoRule.getId());
            }
        }

        if (!matchedRules.isEmpty()) {

            // Note that a request has been embargoed
            WebServerMetrics.EMBARGOED_REQUESTS.inc();

            // Log all the matched rules to Kibana/Ibex
            context.getMetricScope().annotate("embargo", matchedRules);

            // Note which rules have been matched for this request
            for (Long matchedRuleId : matchedRules) {
                logger.trace("Embargo rule id {} has been hit for operation {}, tenantId {}, method {}, " +
                                "userAgent {} and uri {}", matchedRuleId, operation, tenantId, method, userAgent, uri);

                // Increment the counter for the matched rule (create the counter if it doesn't exist)
                WebServerMetrics.EMBARGO_RULE_MATCHES.computeIfAbsent(
                        matchedRuleId, key -> Metrics.counter(String.format("ws.embargo.%s.match", matchedRuleId)))
                        .inc();
            }
            throw new EmbargoException(
                    format("We have temporarily disabled operation: %s," +
                                    " tenantId: %s, method: %s, uri: %s, or userAgent: %s",
                            operation, tenantId, method, uri, userAgent));
        }
    }
    /**
     * This method raises an EmbargoException if the request matches any of the rules in embargoRules
     *
     * The method also, if the request is going to be embargoed:
     * 1) Creates a MetricScope annotation containing all the ids that the request matched
     * 2) Increments a Metrics.Counter for each rule matched, named "ws.embargo.[RULE-ID].match"
     * 3) Increments a Metrics.Counter that reflects the total number of requests embargoed
     *
     * @param metricScope  The metric scope
     * @param operation    The Operation being performed by the request (Can be null prior to Auth)
     * @param tenantId     The tenantId of the requests Principal (can be null prior to Auth)
     * @param method       The HttpMethod of the request
     * @param uri          The uri for the request
     * @param userAgent    The userAgent for the request
     * @param embargoRules The list of embargo rules retrieved from the database
     */
    private void checkForEmbargoRuleMatch(
            MetricScope metricScope, @Nullable String operation, @Nullable String tenantId, String method,
            String uri, String userAgent, Collection<MdsEmbargoRuleV1> embargoRules) {

        final List<Long> matchedRules = Lists.newArrayList();
        for (MdsEmbargoRuleV1 embargoRule : embargoRules) {
            if (EmbargoMatcher.matches(embargoRule, operation, tenantId, uri, method, userAgent)) {
                matchedRules.add(embargoRule.getId());
            }
        }

        if (!matchedRules.isEmpty()) {

            // Note that a request has been embargoed
            WebServerMetrics.EMBARGOED_REQUESTS.inc();

            // Log all the matched rules to Kibana/Ibex
            metricScope.annotate("embargo", matchedRules);

            // Note which rules have been matched for this request
            for (Long matchedRuleId : matchedRules) {
                logger.trace("Embargo rule id {} has been hit for operation {}, tenantId {}, method {}, " +
                        "userAgent {} and uri {}", matchedRuleId, operation, tenantId, method, userAgent, uri);

                // Increment the counter for the matched rule (create the counter if it doesn't exist)
                WebServerMetrics.EMBARGO_RULE_MATCHES.computeIfAbsent(
                        matchedRuleId, key -> Metrics.counter(String.format("ws.embargo.%s.match", matchedRuleId)))
                        .inc();
            }
            throw new EmbargoException(
                    format("We have temporarily disabled operation: %s," +
                                    " tenantId: %s, method: %s, uri: %s, or userAgent: %s",
                            operation, tenantId, method, uri, userAgent));
        }
    }

}
