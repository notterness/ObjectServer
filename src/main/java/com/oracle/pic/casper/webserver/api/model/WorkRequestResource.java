package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.MoreObjects;

import java.util.Objects;

public class WorkRequestResource {

    public enum ActionType {

        Created("CREATED"),
        Updated("UPDATED"),
        Deleted("DELETED"),
        Related("RELATED"),
        InProgress("IN_PROGRESS"),
        Read("READ"),
        Written("WRITTEN");

        private final String value;

        ActionType(String value) {
            this.value = value;
        }

        @JsonValue
        public String getValue() {
            return value;
        }


        @JsonCreator
        public static ActionType create(String key) {
            for (ActionType action : ActionType.values()) {
                if (action.getValue().equals(key)) {
                    return action;
                }
            }
            throw new IllegalStateException(String.format(
                    "Received unknown value '%s' for enum 'ActionType', returning UnknownEnumValue", key
            ));
        }
    }

    static class Metadata {

        private final String region;
        private final String namespace;
        private final String bucket;
        private final String object;

        Metadata(String region, String namespace, String bucket, String object) {
            this.region = region;
            this.namespace = namespace;
            this.bucket = bucket;
            this.object = object;
        }

        @JsonProperty("REGION")
        public String getRegion() {
            return region;
        }

        @JsonProperty("NAMESPACE")
        public String getNamespace() {
            return namespace;
        }

        @JsonProperty("BUCKET")
        public String getBucket() {
            return bucket;
        }

        @JsonProperty("OBJECT")
        public String getObject() {
            return object;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Metadata metadata = (Metadata) o;
            return Objects.equals(region, metadata.region) &&
                Objects.equals(namespace, metadata.namespace) &&
                Objects.equals(bucket, metadata.bucket) &&
                Objects.equals(object, metadata.object);
        }

        @Override
        public int hashCode() {
            return Objects.hash(region, namespace, bucket, object);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("region", region)
                .add("namespace", namespace)
                .add("bucket", bucket)
                .add("object", object)
                .toString();
        }
    }

    private final ActionType actionType;
    private final String entityType;
    private final String identifier;
    private final String entityUri;
    private final Metadata metadata;

    public WorkRequestResource(@JsonProperty("actionType") ActionType actionType,
                               @JsonProperty("entityType") String entityType,
                               @JsonProperty("identifier") String identifier,
                               @JsonProperty("entityUri") String entityUri,
                               @JsonProperty("metadata") Metadata metadata) {
        this.actionType = actionType;
        this.entityType = entityType;
        this.identifier = identifier;
        this.entityUri = entityUri;
        this.metadata = metadata;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getEntityUri() {
        return entityUri;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkRequestResource that = (WorkRequestResource) o;
        return actionType == that.actionType &&
                Objects.equals(entityType, that.entityType) &&
                Objects.equals(identifier, that.identifier) &&
                Objects.equals(entityUri, that.entityUri) &&
                Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionType, entityType, identifier, entityUri, metadata);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("actionType", actionType)
                .add("entityType", entityType)
                .add("identifier", identifier)
                .add("entityUri", entityUri)
                .add("metadata", metadata)
                .toString();
    }

    /**
     * Create a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private ActionType actionType;
        private String entityType;
        private String identifier;
        private String entityUri;
        private Metadata metadata;

        public Builder actionType(ActionType actionType) {
            this.actionType = actionType;
            return this;
        }

        public Builder entityType(String entityType) {
            this.entityType = entityType;
            return this;
        }

        public Builder identifier(String identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder entityUri(String entityUri) {
            this.entityUri = entityUri;
            return this;
        }

        public Builder metadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public WorkRequestResource build() {
            return new WorkRequestResource(actionType, entityType, identifier, entityUri, metadata);
        }
    }

}
