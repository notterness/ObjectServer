package com.oracle.pic.casper.webserver.api.replication;

import com.oracle.bmc.objectstorage.model.ReplicationPolicy;
import com.oracle.pic.casper.common.replication.ReplicationOptions;
import com.oracle.pic.casper.webserver.auth.CasperPermission;
import com.oracle.pic.casper.webserver.api.model.WsReplicationPolicy;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidReplicationOptionsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.ReplicationOptionsConflictException;
import com.oracle.pic.casper.webserver.util.Validator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class ReplicationUtils {

    private ReplicationUtils() {
    }

    public static final List<CasperPermission> REPLICATION_PERMISSIONS;

    static {
        List<CasperPermission> permissions = new ArrayList<>();

        permissions.add(CasperPermission.OBJECT_READ);
        permissions.add(CasperPermission.OBJECT_CREATE);
        permissions.add(CasperPermission.OBJECT_OVERWRITE);
        permissions.add(CasperPermission.OBJECT_INSPECT);
        permissions.add(CasperPermission.OBJECT_DELETE);
        permissions.add(CasperPermission.OBJECT_RESTORE);
        permissions.add(CasperPermission.BUCKET_READ);
        permissions.add(CasperPermission.BUCKET_INSPECT);
        permissions.add(CasperPermission.BUCKET_UPDATE);

        REPLICATION_PERMISSIONS = Collections.unmodifiableList(permissions);
    }

    static ReplicationPolicy toReplicationPolicy(WsReplicationPolicy policy) {
        return ReplicationPolicy.builder()
                .statusMessage(policy.getStatusMessage())
                .timeCreated(Date.from(policy.getTimeCreated()))
                .timeLastSync(Date.from(policy.getTimeLastSync()))
                .status(policy.getStatus())
                .id(policy.getPolicyId())
                .name(policy.getPolicyName())
                .destinationRegionName(policy.getDestRegion())
                .destinationBucketName(policy.getDestBucketName())
                .build();
    }

    public static void validateOptions(String namespace, String bucket,
                                       Map<String, Object> currentOptions,
                                       Map<String, Object> newOptions) {
        if (newOptions == null || newOptions.isEmpty()) {
            throw new InvalidReplicationOptionsException("No replication options are provided");
        }

        validateOption(namespace, bucket, newOptions, currentOptions, ReplicationOptions.XRR_POLICY_ID,
                Validator::validateReplicationPolicyId, null);

        Predicate<Object> consistencyWithPolicyIdCheck =
                o -> Objects.isNull(newOptions.get(ReplicationOptions.XRR_POLICY_ID)) == Objects.isNull(o);

        if (newOptions.size() > ReplicationOptions.XRR_OPTIONS_COUNT) {
            throw new InvalidReplicationOptionsException("Too many replication options are provided");
        }

        if (currentOptions.size() > 0 && currentOptions.size() != newOptions.size()) {
            throw new ReplicationOptionsConflictException("Replication options conflict with existing options.");
        }

        if (newOptions.size() > 1) {
            validateOption(namespace, bucket, newOptions, currentOptions, ReplicationOptions.XRR_POLICY_NAME,
                Validator::validatePolicyName, consistencyWithPolicyIdCheck);
            validateOption(namespace, bucket, newOptions, currentOptions, ReplicationOptions.XRR_SOURCE_REGION,
                    Validator::validateRegion, consistencyWithPolicyIdCheck);
            validateOption(namespace, bucket, newOptions, currentOptions, ReplicationOptions.XRR_SOURCE_BUCKET,
                    Validator::validateBucket, consistencyWithPolicyIdCheck);
            validateOption(namespace, bucket, newOptions, currentOptions, ReplicationOptions.XRR_SOURCE_BUCKET_ID,
                    Validator::validateBucketId, consistencyWithPolicyIdCheck);
        }
    }

    private static void validateOption(String namespace,
                                       String bucket,
                                       Map<String, Object> newOptions,
                                       Map<String, Object> currentOptions,
                                       String option,
                                       Consumer<String> validator,
                                       Predicate<Object> stateCheck) {
        if (!newOptions.containsKey(option)) {
            throw new InvalidReplicationOptionsException(String.format("Missing %s option", option));
        }

        Object newValue = newOptions.get(option);
        if (stateCheck != null && !stateCheck.test(newValue)) {
            throw new InvalidReplicationOptionsException(
                    String.format("Invalid value '%s' for %s option", newValue, option));
        }

        if (newValue != null) {
            validator.accept(newValue.toString());
        }

        if (currentOptions == null || currentOptions.isEmpty()) {
            return;
        }

        Object currentValue = currentOptions.get(option);
        if (currentValue != null && newValue != null && !currentValue.equals(newValue)) {
            throw new ReplicationOptionsConflictException(
                    String.format("Conflict trying to set replication option '%s' in bucket '%s' in namespace '%s', " +
                                    "current value: '%s', new value: '%s'", option, bucket, namespace,
                            currentValue, newValue));
        }
    }

}
