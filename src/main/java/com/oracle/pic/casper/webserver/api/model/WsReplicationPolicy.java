package com.oracle.pic.casper.webserver.api.model;

import com.oracle.bmc.objectstorage.model.ReplicationPolicy;

import java.time.Instant;

public class WsReplicationPolicy {

    private String policyId;
    private String policyName;
    private String sourceTenantOcid;
    private String sourceNamespace;
    private String sourceBucketName;
    private String sourceBucketId;
    private String destRegion;
    private String destBucketName;
    private boolean enabled;
    private ReplicationPolicy.Status status;
    private String statusMessage;
    private Instant timeCreated;
    private Instant timeLastSync;

    public WsReplicationPolicy() {
    }

    public WsReplicationPolicy(WsReplicationPolicy other) {
        this.policyId = other.policyId;
        this.policyName = other.policyName;
        this.sourceTenantOcid = other.sourceTenantOcid;
        this.sourceNamespace = other.sourceNamespace;
        this.sourceBucketName = other.sourceBucketName;
        this.sourceBucketId = other.sourceBucketId;
        this.destRegion = other.destRegion;
        this.destBucketName = other.destBucketName;
        this.enabled = other.enabled;
        this.status = other.status;
        this.statusMessage = other.statusMessage;
    }

    public String getPolicyId() {
        return policyId;
    }

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getSourceTenantOcid() {
        return sourceTenantOcid;
    }

    public void setSourceTenantOcid(String sourceTenantOcid) {
        this.sourceTenantOcid = sourceTenantOcid;
    }

    public String getSourceNamespace() {
        return sourceNamespace;
    }

    public void setSourceNamespace(String sourceNamespace) {
        this.sourceNamespace = sourceNamespace;
    }

    public String getSourceBucketName() {
        return sourceBucketName;
    }

    public void setSourceBucketName(String sourceBucketName) {
        this.sourceBucketName = sourceBucketName;
    }

    public String getSourceBucketId() {
        return sourceBucketId;
    }

    public void setSourceBucketId(String sourceBucketId) {
        this.sourceBucketId = sourceBucketId;
    }

    public String getDestRegion() {
        return destRegion;
    }

    public void setDestRegion(String destRegion) {
        this.destRegion = destRegion;
    }

    public String getDestBucketName() {
        return destBucketName;
    }

    public void setDestBucketName(String destBucketName) {
        this.destBucketName = destBucketName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public ReplicationPolicy.Status getStatus() {
        return status;
    }

    public void setStatus(ReplicationPolicy.Status status) {
        this.status = status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public Instant getTimeCreated() {
        return timeCreated;
    }

    public void setTimeCreated(Instant timeCreated) {
        this.timeCreated = timeCreated;
    }

    public Instant getTimeLastSync() {
        return timeLastSync;
    }

    public void setTimeLastSync(Instant timeLastSync) {
        this.timeLastSync = timeLastSync;
    }
}
