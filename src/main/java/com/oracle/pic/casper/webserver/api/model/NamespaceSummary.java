package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Namespaces collect multiple buckets and map to the tenant compartment of the user.
 * At the moment we only include name however we might extend this to include the information
 * found in {@link com.oracle.pic.casper.objectmeta.NamespaceInfo}
 */
@JsonDeserialize(builder = Bucket.Builder.class)
public class NamespaceSummary {

    @JsonProperty("name")
    private final String name;

    public NamespaceSummary(String name) {
        this.name = name;
    }

    public static NamespaceSummary.Builder builder() {
        return new NamespaceSummary.Builder();
    }

    public static NamespaceSummary.Builder builder(NamespaceSummary copy) {
        return builder().name(copy.name);
    }

    public String getName() {
        return name;
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        @JsonProperty("name")
        private String name;

        public NamespaceSummary.Builder name(String val) {
            name = val;
            return this;
        }

        public String getName() {
            return name;
        }

        public NamespaceSummary build() {
            Preconditions.checkNotNull(name);
            return new NamespaceSummary(name);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamespaceSummary that = (NamespaceSummary) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .toString();
    }
}
