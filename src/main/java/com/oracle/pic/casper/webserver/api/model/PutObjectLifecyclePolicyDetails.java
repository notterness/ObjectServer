package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class PutObjectLifecyclePolicyDetails {


    private final List<ObjectLifecycleRule> items;

    public PutObjectLifecyclePolicyDetails(@JsonProperty("items") List<ObjectLifecycleRule> items) {
        this.items = items;
    }

    public List<ObjectLifecycleRule> getItems() {
        return items;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PutObjectLifecyclePolicyDetails)) return false;
        PutObjectLifecyclePolicyDetails that = (PutObjectLifecyclePolicyDetails) o;
        return Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(items);
    }
}
