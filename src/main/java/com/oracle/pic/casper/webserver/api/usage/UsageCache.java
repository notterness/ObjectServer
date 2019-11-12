package com.oracle.pic.casper.webserver.api.usage;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.oracle.pic.casper.common.config.v2.UsageConfiguration;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.mds.common.grpc.TimestampUtils;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.operator.GetAggregateUsageByNamespaceRequest;
import com.oracle.pic.casper.mds.operator.GetAggregateUsageByNamespaceResponse;
import com.oracle.pic.casper.mds.operator.OperatorMetadataServiceGrpc.OperatorMetadataServiceBlockingStub;
import com.oracle.pic.casper.webserver.api.backend.MdsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class UsageCache {

    public static final Logger LOG = LoggerFactory.getLogger(UsageCache.class);

    /**
     * Cache for compartment storage usage and tenant storage usage per tenant.
     * Key : namespace name
     * Value : a map (key : compartment id or namespace name --> value : storage usage in bytes)
     */
    private final LoadingCache<String, Map<String, Long>> cache;
    private final MdsExecutor<OperatorMetadataServiceBlockingStub> client;
    private final Duration requestDeadline;
    private final Duration maxAge;

    public UsageCache(
        UsageConfiguration usageConfiguration,
        MdsExecutor<OperatorMetadataServiceBlockingStub> client,
        Duration requestDeadline) {
        this(
            usageConfiguration,
            client,
            requestDeadline,
            usageConfiguration.getMaxAge(),
            Ticker.systemTicker());
    }

    UsageCache(
        UsageConfiguration usageConfiguration,
        MdsExecutor<OperatorMetadataServiceBlockingStub> client,
        Duration requestDeadline,
        Duration maxAge,
        Ticker ticker) {
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(usageConfiguration.getCacheConfiguration().getMaximumSize())
            .expireAfterWrite(usageConfiguration.getCacheConfiguration().getExpireAfterWrite())
            .refreshAfterWrite(usageConfiguration.getCacheConfiguration().getRefreshAfterWrite())
            .recordStats()
            .ticker(ticker)
            .build(CacheLoader.from(this::getUsageFromOperatorMds));
        this.client = client;
        this.requestDeadline = requestDeadline;
        this.maxAge = maxAge;

        Metrics.gauge("webServerUsageCache.hitRate", () -> cache.stats().hitRate());
    }

    private Map<String, Long> getUsageFromOperatorMds(String namespace) {
        GetAggregateUsageByNamespaceResponse response;
        response = MdsMetrics.executeWithMetrics(MdsMetrics.OPERATOR_MDS_BUNDLE,
            MdsMetrics.OPERATOR_GET_USAGE,
            false,
            () -> client.execute(c -> c.withDeadlineAfter(requestDeadline.toMillis(), TimeUnit.MILLISECONDS)
                .getAggregateUsageByNamespace(GetAggregateUsageByNamespaceRequest.newBuilder()
                    .setNamespace(namespace)
                    .setMaxAge(TimestampUtils.toProtoDuration(maxAge))
                    .build())));

        return response.getUsageMap();
    }

    public Map<String, Long> getUsageFromCache(String namespace) {
        try {
            return this.cache.getUnchecked(namespace);
        } catch (UncheckedExecutionException e) {
            LOG.trace("Unable to talk to Operator Mds. Will return Empty map (0 usage) for namespace {}", namespace, e);
            return new HashMap<>();
        }
    }
}
