package com.oracle.pic.casper.webserver.auth.limits;

import com.oracle.pic.accounts.model.CompartmentQuotasAndServiceLimits;
import com.oracle.pic.accounts.model.ListCompartmentsQuotasRequest;
import com.oracle.pic.accounts.model.ServiceLimitEntry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class InMemoryLimitsClient implements LimitsClient {

    /**
     * Global suspended value you can set for testing
     */
    private boolean suspended = false;

    /**
     * Default Value for enabling public bucket creation for tenant.
     */
    private boolean isPublicBucketEnabled = true;

    private long maxCopyRequests = 10000;

    private long maxCopyBytes = 1024;

    private Set<String> eventWhitelistedTenancies =  new HashSet<>();

    private long maxBulkRestoreRequests = 10000;

    private long maxBuckets = 100000L;

    private long bucketShards = 1L;

    /**
     * Default Value for max storage capacity for tenant is -1 (No storage capacity enforced).
     */
    private long maxStorageBytes = Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES;

    @Override
    public boolean isSuspended(String tenantId) {
        return suspended;
    }

    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    @Override
    public boolean isPublicBucketEnabled(String tenantId) {
        return isPublicBucketEnabled;
    }

    public void setPublicBucketEnabled(boolean publicBucketEnabled) {
        isPublicBucketEnabled = publicBucketEnabled;
    }

    @Override
    public long getMaxCopyRequests(String tenantId) {
        return maxCopyRequests;
    }

    @Override
    public long getMaxBulkRestoreRequests(String tenantId) {
        return maxBulkRestoreRequests;
    }

    public void setMaxCopyRequests(long maxCopyRequests) {
        this.maxCopyRequests = maxCopyRequests;
    }

    public void setMaxBulkRestoreRequests(long maxBulkRestoreRequests) {
        this.maxBulkRestoreRequests = maxBulkRestoreRequests;
    }

    @Override
    public long getMaxCopyBytes(String tenantId) {
        return maxCopyBytes;
    }

    public void setMaxCopyBytes(long maxCopyBytes) {
        this.maxCopyBytes = maxCopyBytes;
    }

    @Override
    public Set<String> getEventWhitelistedTenancies(String whitelistedService) {
        return eventWhitelistedTenancies;
    }

    public void setEventWhitelistedTenancies(Set<String> whitelist) {
        eventWhitelistedTenancies.addAll(whitelist);
    }

    @Override
    public long getMaxBuckets(String tenantId) {
        return maxBuckets;
    }

    public void setMaxBuckets(long maxBuckets) {
        this.maxBuckets = maxBuckets;
    }

    @Override
    public long getBucketShards(String tenantId) {
        return bucketShards;
    }

    public void setBucketShards(long bucketShards) {
        this.bucketShards = bucketShards;
    }

    @Override
    public long getStorageLimitBytes(String tenantId) {
        return maxStorageBytes;
    }

    @Override
    public CompartmentQuotasAndServiceLimits getQuotasAndServiceLimits(ListCompartmentsQuotasRequest request) {
        ServiceLimitEntry storageLimit = new ServiceLimitEntry("storage-bytes",
                Limits.DEFAULT_NO_STORAGE_LIMIT_BYTES);
        return CompartmentQuotasAndServiceLimits.builder()
                .serviceLimits(Arrays.asList(storageLimit))
                .compartmentQuotas(Arrays.asList())
                .build();
    }

    public void setMaxStorageBytes(long maxStorageBytes) {
        this.maxStorageBytes = maxStorageBytes;
    }

    @Override
    public void close() {

    }
}
