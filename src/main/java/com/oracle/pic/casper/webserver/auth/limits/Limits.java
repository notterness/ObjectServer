package com.oracle.pic.casper.webserver.auth.limits;

import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.oracle.pic.accounts.model.CompartmentQuotasAndServiceLimits;
import com.oracle.pic.accounts.model.ListCompartmentsQuotasRequest;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.failsafe.FailSafeConfig;
import com.oracle.pic.casper.common.config.failsafe.LimitsFailSafeConfig;
import com.oracle.pic.casper.common.config.v2.CacheConfiguration;
import com.oracle.pic.casper.common.config.v2.LimitsClientConfiguration;
import com.oracle.pic.casper.common.exceptions.LimitsServerException;
import com.oracle.pic.casper.common.metrics.Metrics;
import com.oracle.pic.casper.common.metrics.MetricsBundle;
import com.oracle.pic.casper.common.rest.HttpResponseStatus;
import com.oracle.pic.limits.entities.exceptions.CompartmentQuotaException;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *  A caching, retrying interface to the limits service.
 */
public class Limits {
    private static final Logger LOG = LoggerFactory.getLogger(Limits.class);

    // Since the whitelist does not reside in any tenancy this dummy string will be used
    // to create the hashmap and store the value as set of string in LoadingCache
    // In future this mechanism could be used to enable whitelisting tenancies for
    // other service integration as well by reading from limits service
    private static final String EVENTS_LIMITS = "events-whitelist";

    static final int DEFAULT_NUM_SHARDS_PER_BUCKET = 1;
    // Default value for storageLimitBytes, -1 indicates there is no storage limit applied.
    public static final long DEFAULT_NO_STORAGE_LIMIT_BYTES = -1L;

    /**
     * Used to call the limits service.
     */
    private final LimitsClient limitsClient;

    /**
     * Retry policy used when calling the limits service.
     */
    private final RetryPolicy retryPolicy;

    /**
     * Cache for the suspended status of tenancies.
     */
    private final LoadingCache<String, Boolean> isSuspendedCache;

    /**
     * Cache for the public bucket status of tenancies.
     */
    private final LoadingCache<String, Boolean> isPublicBucketEnabledCache;

    /**
     * Cache for the maximum number of copy requests per tenant.
     */
    private final LoadingCache<String, Long> maxCopyRequestCache;

    /**
     * Cache for the maximum number of bulk restore requests per tenant.
     */
    private final LoadingCache<String, Long> maxBulkRestoreRequestCache;

    /**
     * Cache for the maximum number of copy bytes per tenant.
     */
    private final LoadingCache<String, Long> maxCopyBytesCache;

    /**
     * Cache for whitelisted tenancies for events service
     */
    private final LoadingCache<String, Set<String>> eventsWhitelistTenancies;

    /**
     * Cache for the maximum number of buckets per tenant.
     */
    private final LoadingCache<String, Long> maxBucketsCache;

    /**
     * Cache for the maximum number of storage capacity per tenant.
     */
    private final LoadingCache<String, Long> storageLimitBytesCache;

    /**
     * Cache for the service limits and quota values per quota request.
     */
    private final LoadingCache<ListCompartmentsQuotasRequest, CompartmentQuotasAndServiceLimits> quotaCache;

    /**
     * Cache for the number of shards a bucket should be created with per tenant.
     */
    private final LoadingCache<String, Long> numShardsCache;

    private final long defaultMaxCopyRequests;

    private final long defaultMaxCopyBytes;

    private final long defaultMaxBulkRestoreRequests;

    private final long defaultMaxBuckets;

    /**
     *
     * FailSafe Config for limits service.
     */
    private final FailSafeConfig failSafeConfig;


    Limits(
        ConfigRegion region,
        LimitsClient limitsClient,
        Ticker ticker,
        long defaultMaxCopyRequests,
        long defaultMaxBulkRestoreRequests,
        long defaultMaxCopyBytes,
        long defaultMaxBuckets,
        LimitsClientConfiguration config) {
        Preconditions.checkNotNull(region);
        Preconditions.checkNotNull(ticker);
        this.limitsClient = Preconditions.checkNotNull(limitsClient);

        // Number of entries to cache. Limits are per-tenancy so we want a cache
        // that will hold most of our active tenants.
        CacheConfiguration cacheConfiguration = config.getCacheConfiguration();
        long cacheSize = cacheConfiguration.getMaximumSize();

        this.failSafeConfig = new LimitsFailSafeConfig(config.getFailSafeConfiguration());
        this.retryPolicy = failSafeConfig.getRetryPolicy();

        // For our cache we have two time limits:
        //  1) Refresh time. Once a cache entry hits this age the first thread
        //     that reads the entry will attempt to get a new value from the
        //     limits service. Other threads will continue to use the old value.
        //     If the attempt to refresh the cache fails the old value is returned.
        //  2) Expiry time. Once a cache entry hits this age it will be removed
        //     and all requests for the cache entry will have to call the limits
        //     service, or wait for another thread that is calling the limits service.
        // The gap in between the refresh and expiry times is the "grace period".

        // For pre-prod testing we will make sure that suspension takes
        // effect quickly.

        // In production regions we will refresh the cache less frequently
        // to reduce the load on the limits service and use a large grace
        // period. We limit the length of the grace period so that if we
        // have cached a "true" value we will eventually fail open for
        // that tenancy.

        final Duration cacheRefresh = cacheConfiguration.getRefreshAfterWrite();
        final Duration cacheExpiration = cacheConfiguration.getExpireAfterWrite();

        this.isSuspendedCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::getSuspendedStatusWithRetry));
        this.isPublicBucketEnabledCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::getPublicBucketStatusWithRetry));
        this.maxCopyRequestCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::getMaxCopyRequestsWithRetry));
        this.maxBulkRestoreRequestCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::getMaxBulkRestoreRequestsWithRetry));
        this.maxCopyBytesCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::getMaxCopyBytesWithRetry));
        this.maxBucketsCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::getMaxBucketsWithRetry));
        this.numShardsCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::getNumShardsWithRetry));
        this.storageLimitBytesCache = CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
            .ticker(ticker)
            .maximumSize(cacheSize)
            .build(CacheLoader.from(this::getStorageLimitBytesWithRetry));
        Metrics.gauge("limits.storageLimitBytesCache.hitRate",
                () -> this.storageLimitBytesCache.stats().hitRate());

        CacheConfiguration quotaCache = config.getQuotaConfiguration().getCacheConfiguration();
        long quotaCacheSize = quotaCache.getMaximumSize();
        final Duration quotaCacheRefresh = quotaCache.getRefreshAfterWrite();
        final Duration quotaCacheExpiration = quotaCache.getExpireAfterWrite();
        this.quotaCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(quotaCacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(quotaCacheExpiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(quotaCacheSize)
                .build(CacheLoader.from(this::getQuotasAndServiceLimitsWithRetry));
        Metrics.gauge("limits.quotaCache.hitRate", () -> this.quotaCache.stats().hitRate());

        // increasing expireAfterWrite time as we would not like to return empty set
        // if we cannot connect to limit service. Setting grace period to 24 hours
        this.eventsWhitelistTenancies = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheRefresh.toMillis(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(cacheExpiration.toMillis() + TimeUnit.DAYS.toMillis(1), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .maximumSize(cacheSize)
                .build(CacheLoader.from(this::getEventsWhitelistedTenanciesWithRetry));

        this.defaultMaxCopyRequests = defaultMaxCopyRequests;
        this.defaultMaxCopyBytes =  defaultMaxCopyBytes;
        this.defaultMaxBulkRestoreRequests = defaultMaxBulkRestoreRequests;
        this.defaultMaxBuckets = defaultMaxBuckets;
    }

    public Limits(
        ConfigRegion region,
        LimitsClient limitsClient,
        long defaultMaxCopyRequests,
        long defaultMaxBulkRestoreRequests,
        long defaultMaxCopyBytes,
        long defaultMaxBuckets,
        LimitsClientConfiguration config) {
        this(
            region,
            limitsClient,
            Ticker.systemTicker(),
            defaultMaxCopyRequests,
            defaultMaxBulkRestoreRequests,
            defaultMaxCopyBytes,
            defaultMaxBuckets,
            config);
    }

    /**
     * Return true if we know that the tenancy is suspended.
     */
    public boolean isSuspended(String tenant) {
        try {
            return this.isSuspendedCache.getUnchecked(tenant);
        } catch (UncheckedExecutionException ex) {
            if (ex.getCause() instanceof LimitsServerException) {
                final LimitsServerException rex = (LimitsServerException) ex.getCause();
                if (rex.getStatusCode() == HttpResponseStatus.NOT_FOUND) {
                    LOG.debug("The properties service returned a 404 status for tenant {}", tenant);
                    // The limits service has indicated that it does not know about this tenant ID, which means
                    // that either: the tenant ID does not exist, the limits service doesn't know about it yet
                    // (maybe it was just created), or there is a bug causing the limits service to return a
                    // 404. In the last two cases, we'd prefer to not cache, but to fail closed, so we go ahead
                    // and return true here. The danger here is that we introduce more load on the service if it
                    // returning a lot of 404s, so this is something we will want to revisit in the future.
                    return true;
                }
            }
            // What happens if we fail to talk to the limits service? We have
            // the choice of failing open (assume the tenancy is NOT suspended)
            // or failing closed (assume the tenancy IS suspended). Failing
            // open will cause far fewer problems so we return false as a default.
            LOG.trace("Unable to talk to limits service. Failing open for tenant {}", tenant, ex);
            return false;
        }
    }

    /**
     * Return true if we know that the tenancy is allowed to create public bucket.
     */
    public boolean isPublicBucketEnabled(String tenant) {
        try {
            return this.isPublicBucketEnabledCache.getUnchecked(tenant);
        } catch (UncheckedExecutionException ex) {
            // What happens if we fail to talk to the limits service? We have
            // the choice of failing open (assume the tenancy is allowed to create public bucket)
            // or failing closed (assume the tenancy is not allowed to create public bucket). Failing
            // closed will cause far fewer problems(security) so we return false as a default.
            LOG.trace("Unable to talk to limits service. Failing closed for tenant {}", tenant, ex);
            return false;
        }
    }

    /**
     * Return the maximum number of copy requests allowed for that tenant.
     */
    public long getMaxCopyRequests(String tenant) {
        try {
            return this.maxCopyRequestCache.getUnchecked(tenant);
        } catch (UncheckedExecutionException e) {
            //Default to the default max copy requests if we fail to talk to limits service.
            //The worst that can happen is when we have disabled copy for a tenant (set limit to 0) but re-enable them
            // if limits service is down.  This is not likely, and there are other ways to disable copy for tenant such
            // as Embargo.  Alternatively we can return 0 to be safe, but that will not let anyone make copy requests
            // if limits service is down.
            LOG.trace("Unable to talk to limits service. Using default max copy requests value", tenant, e);
            return defaultMaxCopyRequests;
        }
    }

    /**
     * Return the maximum number of bulk restore requests allowed for that tenant.
     */
    public long getMaxBulkRestoreRequests(String tenant) {
        try {
            return this.maxBulkRestoreRequestCache.getUnchecked(tenant);
        } catch (UncheckedExecutionException e) {
            //Default to the default max bulk restore requests if we fail to talk to limits service.
            //The worst that can happen is when we have disabled bulk restore for a tenant (set limit to 0) but
            // re-enable them if limits service is down.  This is not likely, and there are other ways to disable bulk
            // restore for tenant such as Embargo.  Alternatively we can return 0 to be safe, but that will not let
            // anyone make bulk restore requests if limits service is down.
            LOG.trace("Unable to talk to limits service. Using default max bulk restore requests value", tenant, e);
            return defaultMaxBulkRestoreRequests;
        }
    }


    /**
     * Return the maximum number of copy requests allowed for that tenant.
     */
    public long getMaxCopyBytes(String tenant) {
        try {
            return this.maxCopyBytesCache.getUnchecked(tenant);
        } catch (UncheckedExecutionException e) {
            //Default to the default max copy requests if we fail to talk to limits service.
            //The worst that can happen is when we have disabled copy for a tenant (set limit to 0) but re-enable them
            // if limits service is down.  This is not likely, and there are other ways to disable copy for tenant such
            // as Embargo.  Alternatively we can return 0 to be safe, but that will not let anyone make copy requests
            // if limits service is down.
            LOG.trace("Unable to talk to limits service. Using default max copy requests value", tenant, e);
            return defaultMaxCopyBytes;
        }
    }

    /**
     * Return the list of whitelisted tenancies for events service integration.
     */
    public Set<String> getEventsWhitelistedTenancies() {
        try {
            return this.eventsWhitelistTenancies.getUnchecked(EVENTS_LIMITS);
        } catch (UncheckedExecutionException ex) {
            // What happens if we fail to talk to the limits service? We have
            // the choice of failing open (assume the tenancy is whitelisted)
            // or failing closed (assume the tenancy is not whitelisted). Failing
            // closed will cause far fewer problems(scalability issues if we enable everyone)
            // so we return with empty set.
            LOG.trace("Unable to talk to limits service. Failing closed due to ", ex);
            // Grace period has been set to 24 hours so that we avoid returning empty set
            return Collections.emptySet();
        }
    }

    /**
     * Return the maximum number of buckets allowed for a tenant.
     */
    public long getMaxBuckets(String tenant) {
        try {
            return this.maxBucketsCache.getUnchecked(tenant);
        } catch (UncheckedExecutionException e) {
            // return default max bucket limits if we fail to talk to limits service.
            LOG.trace(
                "Unable to talk to limits service to get max buckets for {}. Using default value {}",
                tenant,
                defaultMaxBuckets,
                e);
            return defaultMaxBuckets;
        }
    }

    public long getNumShards(String tenant) {
        try {
            return this.numShardsCache.getUnchecked(tenant);
        } catch (UncheckedExecutionException e) {
            // return default num shards per bucket limit if we fail to talk to limits service.
            LOG.trace(
                    "Unable to talk to limits service to get num shards per bucket for {}. Using default value {}",
                    tenant,
                    DEFAULT_NUM_SHARDS_PER_BUCKET,
                    e);
            return DEFAULT_NUM_SHARDS_PER_BUCKET;
        }
    }

    /**
     * Return the maximum number of storage capacity allowed for a tenant.
     */
    public long getStorageLimitBytes(String tenant) {
        try {
            return this.storageLimitBytesCache.getUnchecked(tenant);
        } catch (UncheckedExecutionException e) {
            LOG.trace(
                "Unable to talk to limits service to get storage limit for {}. Applying no limits",
                tenant,
                e);
            return DEFAULT_NO_STORAGE_LIMIT_BYTES;
        }
    }

    /**
     * Return the service limits and quota values for the requsted tenancy and compartments.
     */
    public CompartmentQuotasAndServiceLimits getQuotasAndServiceLimits(ListCompartmentsQuotasRequest request)
            throws CompartmentQuotaException {
        try {
            return this.quotaCache.getUnchecked(request);
        } catch (UncheckedExecutionException e) {
            throw new CompartmentQuotaException(e.getMessage(), e);
        }
    }

    /**
     * Call the limits service to determine if a tenancy is suspended.
     */
    private boolean getSuspendedStatusWithRetry(String tenant) {
        return Failsafe
            .with(this.retryPolicy)
            .get(() -> this.getWithMetrics(LimitsMetrics.IS_SUSPENDED, () ->
                limitsClient.isSuspended(tenant)));
    }

    /**
     * Call the limits service to determine if a tenancy is allowed to create public bucket.
     */
    private boolean getPublicBucketStatusWithRetry(String tenant) {
        return Failsafe
                .with(this.retryPolicy)
                .get(() -> this.getWithMetrics(LimitsMetrics.IS_PUBLICBUCKET_ENABLED, () ->
                    limitsClient.isPublicBucketEnabled(tenant)));
    }

    private long getMaxCopyRequestsWithRetry(String tenant) {
        return Failsafe
            .with(this.retryPolicy)
            .get(() -> this.getWithMetrics(LimitsMetrics.MAX_COPY_REQUESTS, () ->
                limitsClient.getMaxCopyRequests(tenant)));
    }

    private long getMaxBulkRestoreRequestsWithRetry(String tenant) {
        return Failsafe
                .with(this.retryPolicy)
                .get(() -> this.getWithMetrics(LimitsMetrics.MAX_BULK_RESTORE_REQUESTS, () ->
                        limitsClient.getMaxBulkRestoreRequests(tenant)));
    }

    private long getMaxCopyBytesWithRetry(String tenant) {
        return Failsafe
            .with(this.retryPolicy)
            .get(() -> this.getWithMetrics(LimitsMetrics.MAX_COPY_BYTES, () ->
                limitsClient.getMaxCopyBytes(tenant)));
    }

    private Set<String> getEventsWhitelistedTenanciesWithRetry(String whitelistedService) {
        return Failsafe
                .with(this.retryPolicy)
                .get(() -> this.getWithMetrics(LimitsMetrics.EVENTS_WHITELISTED_TENANCIES, () ->
                        limitsClient.getEventWhitelistedTenancies(whitelistedService)));
    }

    private long getMaxBucketsWithRetry(String tenant) {
        return Failsafe
            .with(this.retryPolicy)
            .get(() -> this.getWithMetrics(LimitsMetrics.MAX_BUCKETS, () ->
                limitsClient.getMaxBuckets(tenant)));
    }

    private long getNumShardsWithRetry(String tenant) {
        return Failsafe
                .with(this.retryPolicy)
                .get(() -> this.getWithMetrics(LimitsMetrics.NUM_SHARDS, () ->
                        limitsClient.getBucketShards(tenant)));
    }

    private long getStorageLimitBytesWithRetry(String tenant) {
        return Failsafe
            .with(this.retryPolicy)
            .get(() -> this.getWithMetrics(LimitsMetrics.STORAGE_LIMIT_BYTES, () ->
                limitsClient.getStorageLimitBytes(tenant)));
    }

    private CompartmentQuotasAndServiceLimits getQuotasAndServiceLimitsWithRetry(
            ListCompartmentsQuotasRequest request) {
        return Failsafe
                .with(this.retryPolicy)
                .get(() -> this.getWithMetrics(LimitsMetrics.SERVICE_LIMITS_QUOTA, () ->
                        limitsClient.getQuotasAndServiceLimits(request)));
    }

    private <T> T getWithMetrics(MetricsBundle metrics, Callable<T> callable) throws Exception {
        boolean success = false;
        final long start = System.nanoTime();
        try {
            metrics.getRequests().inc();
            T result = callable.call();
            success = true;
            return result;
        } finally {
            final long end = System.nanoTime();
            final long elapsed = end - start;
            metrics.getOverallLatency().update(elapsed);
            if (success) {
                metrics.getSuccesses().inc();
            } else {
                metrics.getServerErrors().inc();
            }
        }
    }

    public LimitsClient getLimitsClient() {
        return limitsClient;
    }
}
