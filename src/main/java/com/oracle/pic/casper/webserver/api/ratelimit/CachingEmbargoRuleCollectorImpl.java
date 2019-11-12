package com.oracle.pic.casper.webserver.api.ratelimit;

import com.oracle.pic.casper.common.config.v2.EmbargoConfiguration;
import com.oracle.pic.casper.common.util.CachingSupplierWithAsyncRefresh;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.operator.MdsEmbargoRuleV1;
import com.oracle.pic.casper.mds.operator.OperatorMetadataServiceGrpc.OperatorMetadataServiceBlockingStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Based on the cache refresh interval outlined in {@link EmbargoConfiguration} this class, memoizes the
 * {@link MdsEmbargoRuleV1} fetched from operator mds.
 */
public class CachingEmbargoRuleCollectorImpl extends MdsEmbargoRuleCollectorImpl {
    private static final Logger LOG = LoggerFactory.getLogger(CachingEmbargoRuleCollectorImpl.class);

    @Deprecated
    private final CachingSupplierWithAsyncRefresh<Collection<MdsEmbargoRuleV1>> cacheWithAsyncRefreshedRules;

    private final CachingSupplierWithAsyncRefresh<Collection<EmbargoV3Rule>> cachedDryRunRules;
    private final CachingSupplierWithAsyncRefresh<Collection<EmbargoV3Rule>> cachedEnabledRules;

    public CachingEmbargoRuleCollectorImpl(
            EmbargoConfiguration config,
            MdsExecutor<OperatorMetadataServiceBlockingStub> client) {
        super(client);
        this.cacheWithAsyncRefreshedRules =
                CachingSupplierWithAsyncRefresh.cacheWithAsyncRefresh(super::getEmbargoRules,
                        new ArrayList<>(),
                        config.getRefreshCacheInterval(),
                        "CachingEmbargoRuleCollectorImpl-rule-refresh-thread",
                        "embargo.rule.refresh.task");
        this.cachedDryRunRules =
            CachingSupplierWithAsyncRefresh.cacheWithAsyncRefresh(super::getDryRunRules,
                new ArrayList<>(),
                config.getRefreshCacheInterval(),
                "CachingEmbargoRuleCollectorImpl-dry-run-rule-refresh-thread",
                "embargo.rule.refresh.task");
        this.cachedEnabledRules =
            CachingSupplierWithAsyncRefresh.cacheWithAsyncRefresh(super::getEnabledRules,
                new ArrayList<>(),
                config.getRefreshCacheInterval(),
                "CachingEmbargoRuleCollectorImpl-enabled-rule-refresh-thread",
                "embargo.rule.refresh.task");
    }

    @Override
    public Collection<MdsEmbargoRuleV1> getEmbargoRules() {
        return cacheWithAsyncRefreshedRules.get();
    }

    @Override
    public Collection<EmbargoV3Rule> getDryRunRules() {
        return cachedDryRunRules.get();
    }

    @Override
    public Collection<EmbargoV3Rule> getEnabledRules() {
        return cachedEnabledRules.get();
    }

    /**
     * Refresh the rules -- will make a call to the database
     */
    public void refresh() {
        cacheWithAsyncRefreshedRules.refresh();
        cachedEnabledRules.refresh();
        cachedEnabledRules.refresh();
    }

    public void shutdown() {
        for (CachingSupplierWithAsyncRefresh<?> supplier : new CachingSupplierWithAsyncRefresh<?>[]
                {cacheWithAsyncRefreshedRules, cachedDryRunRules, cachedDryRunRules}) {
            try {
                supplier.shutdown();
            } catch (Throwable t) {
                LOG.warn("Exception thrown while shutting down {}", supplier, t);
            }
        }
    }
}
