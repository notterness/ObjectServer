package com.oracle.pic.casper.webserver.api.eventing;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.events.model.BaseEvent;
import com.oracle.pic.events.model.EventV01;

import java.sql.Date;
import java.time.Instant;
import java.util.UUID;

public abstract class CasperEvent {

    private static final String SOURCE_TEMPLATE = "/service/objectstorage/resourceType/%s";

    @JsonIgnore
    private final Api api;
    @JsonIgnore
    private final ResourceType resourceType;
    @JsonIgnore
    private final EventAction action;
    @JsonIgnore
    private final Instant timestamp;
    @JsonIgnore
    private final String compartmentId;

    private final String namespace;
    private final String displayName;
    private final String eTag;

    CasperEvent(Api api,
                String compartmentId,
                String namespace,
                String displayName,
                String eTag,
                ResourceType eventType,
                EventAction action,
                Instant creationTime) {
        this.api = api;
        this.compartmentId = compartmentId;
        this.namespace = namespace;
        this.displayName = displayName;
        this.eTag = eTag;
        this.resourceType = eventType;
        this.action = action;
        this.timestamp = creationTime;
    }

    public Api getApi() {
        return api;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public EventAction getAction() {
        return action;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getCompartmentId() {
        return compartmentId;
    }

    public String geteTag() {
        return eTag;
    }

    public abstract String getBucketName();

    public BaseEvent toCloudEvent() {
        return EventV01.builder()
            .contentType("application/json")
            .data(this)
            .eventID(UUID.randomUUID().toString())
            .eventTime(Date.from(timestamp))
            .eventType(getCloudEventType())
            .eventTypeVersion("1.0")
            .extensions(ImmutableMap.of("compartmentId", compartmentId))
            .source(getEventSource())
            .build();
    }

    String getCloudEventType() {
        return String.format("com.oraclecloud.objectstorage.%s.%s",
                             resourceType.toString(),
                             action.toString()).toLowerCase();
    }

    String getEventSource() {
        return String.format(SOURCE_TEMPLATE, resourceType.toString()).toLowerCase();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("compartmentOCID", compartmentId)
                .add("namespace", namespace)
                .add("displayName", displayName)
                .add("resourceType", resourceType)
                .add("timestamp", timestamp)
                .add("action", action).toString();
    }
}
