package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidNamespaceMetadataException;

@JsonPropertyOrder({"defaultS3CompartmentId", "defaultSwiftCompartmentId"})
@JsonDeserialize(builder = NamespaceMetadata.Builder.class)
public class NamespaceMetadata {

    private final String namespace;
    private final String defaultS3CompartmentId;
    private final String defaultSwiftCompartmentId;

    public NamespaceMetadata(String defaultS3CompartmentId, String defaultSwiftCompartmentId, String namespaceKey) {
        // Precondition check for update default s3/swift compartmentId.
        if ((defaultS3CompartmentId != null && defaultS3CompartmentId.length() == 0)) {
            throw new InvalidNamespaceMetadataException("The default s3 compartmentID must be a nonempty string");
        }
        if ((defaultSwiftCompartmentId != null && defaultS3CompartmentId.length() == 0)) {
            throw new InvalidNamespaceMetadataException("The default swift compartmentID " +
                    "must be a nonempty string");
        }
        this.defaultS3CompartmentId = defaultS3CompartmentId;
        this.defaultSwiftCompartmentId = defaultSwiftCompartmentId;
        this.namespace = namespaceKey;
    }

    public String getDefaultS3CompartmentId() {
        return this.defaultS3CompartmentId;
    }

    public String getDefaultSwiftCompartmentId() {
        return this.defaultSwiftCompartmentId;
    }

    public String getNamespace() {
        return this.namespace;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        @JsonProperty("defaultS3CompartmentId")
        private String defaultS3CompartmentId;
        @JsonProperty("defaultSwiftCompartmentId")
        private String defaultSwiftCompartmentId;
        @JsonProperty("namespace")
        private String namespace;

        public Builder defaultS3CompartmentId(String val) {
            defaultS3CompartmentId = val;
            return this;
        }

        public Builder defaultSwiftCompartmentId(String val) {
            defaultSwiftCompartmentId = val;
            return this;
        }

        public Builder namespaceKey(String val) {
            namespace = val;
            return this;
        }

        public NamespaceMetadata build() {
            return new NamespaceMetadata(defaultS3CompartmentId, defaultSwiftCompartmentId, namespace);
        }

    }
}
