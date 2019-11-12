package com.oracle.pic.casper.webserver.api.eventing;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.model.WSTenantBucketInfo;

import java.time.Instant;
import java.util.Map;

public class CasperObjectEvent extends CasperEvent {

    private String bucketId;
    private String bucketName;
    private Map<String, String> bucketFreeformTags;
    private Map<String, Map<String, Object>> bucketDefinedTags;
    private String archivalState;


    private CasperObjectEvent(
            Api api,
            String compartmentId,
            String namespace,
            String displayName,
            String eTag,
            String bucketId,
            String bucketName,
            Map<String, String> bucketFreeformTags,
            Map<String, Map<String, Object>> bucketDefinedTags,
            ArchivalState archivalState,
            ResourceType eventType,
            EventAction action,
            Instant creationTime) {
        super(api, compartmentId, namespace, displayName, eTag, eventType, action, creationTime);
        this.bucketId = bucketId;
        this.bucketName = bucketName;
        this.bucketFreeformTags = bucketFreeformTags;
        this.bucketDefinedTags = bucketDefinedTags;
        this.archivalState = archivalState.getState();
    }

    public CasperObjectEvent(
        DecidingKeyManagementService kms,
        WSTenantBucketInfo bucket,
        String objectName,
        String etag,
        ArchivalState archivalState,
        EventAction action) {
        this(bucket.getNamespaceKey().getApi(),
             bucket.getCompartment(),
             bucket.getNamespaceKey().getName(),
             objectName,
             etag,
             bucket.getOcid(),
             bucket.getBucketName(),
             bucket.getTags().getFreeformTags(),
             bucket.getTags().getDefinedTags(),
             archivalState,
             ResourceType.OBJECT,
             action,
             Instant.now());
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Map<String, String> getBucketFreeformTags() {
        return bucketFreeformTags;
    }

    public Map<String, Map<String, Object>> getBucketDefinedTags() {
        return bucketDefinedTags;
    }

    public String getArchivalState() {
        return archivalState;
    }
}
