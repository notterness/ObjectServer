package com.oracle.pic.casper.webserver.auth.limits;

import com.oracle.pic.casper.common.metrics.MetricsBundle;

final class LimitsMetrics {

    private LimitsMetrics() {
    }

    public static final MetricsBundle IS_SUSPENDED
            = new MetricsBundle("limits.issuspended");
    public static final MetricsBundle IS_PUBLICBUCKET_ENABLED
            = new MetricsBundle("limits.ispublicbucket");
    public static final MetricsBundle MAX_COPY_REQUESTS
            = new MetricsBundle("limits.maxcopyrequests");
    public static final MetricsBundle MAX_BULK_RESTORE_REQUESTS
            = new MetricsBundle("limits.maxbulkrestorerequests");
    public static final MetricsBundle MAX_COPY_BYTES
            = new MetricsBundle("limits.maxcopybytes");
    public static final MetricsBundle EVENTS_WHITELISTED_TENANCIES
            = new MetricsBundle("limits.eventswhitelistedtenancies");
    public static final MetricsBundle MAX_BUCKETS
            = new MetricsBundle("limits.maxbuckets");
    public static final MetricsBundle NUM_SHARDS
            = new MetricsBundle("limits.numshards");
    public static final MetricsBundle STORAGE_LIMIT_BYTES
        = new MetricsBundle("limits.storagelimitbytes");
    public static final MetricsBundle SERVICE_LIMITS_QUOTA
            = new MetricsBundle("limits.quota");
}
