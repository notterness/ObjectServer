package com.oracle.pic.casper.webserver.api.replication;

import com.oracle.pic.casper.common.replication.ReplicationOptions;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;
import com.oracle.pic.casper.webserver.api.model.exceptions.BucketReadOnlyException;
import com.oracle.pic.casper.webserver.api.model.exceptions.BucketReplicationEnabledException;
import com.oracle.pic.casper.webserver.api.model.exceptions.ReplicationPolicyConflictException;
import io.vertx.ext.web.RoutingContext;

public interface ReplicationEnforcer {

    void enforce();

    static void throwIfReadOnlyButNotReplicationScope(RoutingContext context,
                                                      WSTenantBucketInfo tenantBucketInfo) {
        throwIfReadOnly(context, tenantBucketInfo, true);
    }

    static void throwIfReadOnly(RoutingContext context,
                                WSTenantBucketInfo tenantBucketInfo) {
        throwIfReadOnly(context, tenantBucketInfo, false);
    }

    static void throwIfReadOnly(RoutingContext context,
                                WSTenantBucketInfo tenantBucketInfo, boolean checkReplicationHeaders) {
        String requestPolicyId = context.request().getHeader(ReplicationOptions.XRR_POLICY_ID);
        if (tenantBucketInfo.isReadOnly()) {
            if (!checkReplicationHeaders || requestPolicyId == null) {
                throw new BucketReadOnlyException(String.format("Bucket '%s' in namespace '%s' is in read-only state",
                        tenantBucketInfo.getBucketName(), tenantBucketInfo.getNamespaceKey().getName()));
            }

            Object setPolicyId = tenantBucketInfo.getOptions().get(ReplicationOptions.XRR_POLICY_ID);
            if (!setPolicyId.equals(requestPolicyId)) {
                throw new ReplicationPolicyConflictException(
                        String.format("Bucket '%s' in namespace '%s' is not configured with the policy id '%s' ",
                                tenantBucketInfo.getBucketName(), tenantBucketInfo.getNamespaceKey().getName(),
                                requestPolicyId));
            }
        } else if (requestPolicyId != null) {
            throw new ReplicationPolicyConflictException(
                    String.format("Bucket '%s' in namespace '%s' is not configured with any replication policy",
                            tenantBucketInfo.getBucketName(), tenantBucketInfo.getNamespaceKey().getName()));
        }
    }

    static void throwIfReplicationEnabled(String bucketName, WSTenantBucketInfo tenantBucketInfo) {
        if (tenantBucketInfo.isReplicationEnabled()) {
            throw new BucketReplicationEnabledException("Bucket named '" + bucketName + "' has replication enabled. ");
        }
    }
}
