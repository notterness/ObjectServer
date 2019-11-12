package com.oracle.pic.casper.webserver.auth.limits;

import com.oracle.pic.accounts.model.CompartmentQuotasAndServiceLimits;
import com.oracle.pic.accounts.model.ListCompartmentsQuotasRequest;

import java.io.Closeable;

import java.util.Set;

public interface LimitsClient extends Closeable {

    boolean isSuspended(String tenantId);

    boolean isPublicBucketEnabled(String tenantId);

    long getMaxCopyRequests(String tenantId);

    long getMaxCopyBytes(String tenantId);

    Set<String> getEventWhitelistedTenancies(String whitelistedService);

    long getMaxBulkRestoreRequests(String tenantId);

    long getMaxBuckets(String tenantId);

    long getBucketShards(String tenantId);

    long getStorageLimitBytes(String tenantId);

    CompartmentQuotasAndServiceLimits getQuotasAndServiceLimits(ListCompartmentsQuotasRequest request);
}
