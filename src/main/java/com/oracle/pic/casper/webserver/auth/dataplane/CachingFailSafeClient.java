package com.oracle.pic.casper.webserver.auth.dataplane;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.oracle.pic.casper.common.config.failsafe.FailSafeConfig;
import com.oracle.pic.casper.common.config.v2.CacheConfiguration;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import net.jodah.failsafe.Failsafe;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * Client which does retries, circuit breaker and caching.
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
abstract class CachingFailSafeClient<K, V> {

    private final LoadingCache<K, V> kvLoadingCache;
    private final FailSafeConfig failSafeConfig;
    private final MetricsBundle metricsBundle;

    CachingFailSafeClient(CacheConfiguration cacheConfiguration,
                          FailSafeConfig failSafeConfig,
                          MetricsBundle metricsBundle,
                          String cacheSizeMetricName) {
        this.failSafeConfig = failSafeConfig;
        this.metricsBundle = metricsBundle;
        this.kvLoadingCache = CacheBuilder.newBuilder()
                .maximumSize(cacheConfiguration.getMaximumSize())
                .refreshAfterWrite(cacheConfiguration.getRefreshAfterWrite().toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheConfiguration.getExpireAfterWrite().toMillis(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<K, V>() {
                    public V load(@Nonnull K key) {
                        return loadValueFailSafe(key);
                    }
                });
        Metrics.gauge(cacheSizeMetricName, kvLoadingCache::size);
    }

    /**
     * Get value from cache. Loads the cache if needed.
     */
    protected V getValue(K key) {
        return kvLoadingCache.getUnchecked(key);
    }

    /**
     * Load value to cache using retries and circuit breaker.
     */
    private V loadValueFailSafe(K key) {
        return Failsafe.with(failSafeConfig.getRetryPolicy())
                .get(() -> loadValueWithMetrics(key));
    }

    /**
     * Load value to the cache updating metrics.
     */
    private V loadValueWithMetrics(K key) {
        final long start = System.nanoTime();
        try {
            metricsBundle.getRequests().inc();
            final V result = loadValue(key);
            metricsBundle.getSuccesses().inc();
            return result;
        } catch (Exception ex) {
            if (isClientError(ex)) {
                metricsBundle.getClientErrors().inc();
            } else {
                metricsBundle.getServerErrors().inc();
            }
            throw ex;
        } finally {
            metricsBundle.getOverallLatency().update(System.nanoTime() - start);
        }
    }

    /**
     * Load value to the cache by fetching it from a server end point.
     */
    protected abstract V loadValue(K key);

    /**
     * Decide whether the given error is a client one or not. This helps in marking the correct metrics.
     */
    protected abstract boolean isClientError(Exception ex);
}
