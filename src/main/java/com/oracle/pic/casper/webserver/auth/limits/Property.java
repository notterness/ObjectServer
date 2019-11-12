package com.oracle.pic.casper.webserver.auth.limits;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Refer to https://bitbucket.oci.oraclecorp.com/projects/LIM/repos/properties-runtime-client/browse/properties-runtime-api-spec/src/main/resources/properties-runtime-api-spec.yaml
 * for the definition of CacheValue.
 */
public class Property {

    private String type;
    private Long min;
    private Long max;
    private String value;

    public Property(@JsonProperty("type") String type,
                    @JsonProperty("min") Long min,
                    @JsonProperty("max") Long max,
                    @JsonProperty("value") String value) {
        this.type = type;
        this.min = min;
        this.max = max;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public Long getMin() {
        return min;
    }

    public Long getMax() {
        return max;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Property that = (Property) o;
        return Objects.equal(type, that.type) &&
            Objects.equal(min, that.min) &&
            Objects.equal(max, that.max) &&
            Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type, min, max, value);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("min", min)
            .add("max", max)
            .add("value", value)
            .toString();
    }
}
