package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class ObjectLifecyclePolicy {

    private final Instant timeCreated;
    private final List<ObjectLifecycleRule> items;

    public ObjectLifecyclePolicy(@JsonProperty("timeCreated") Instant timeCreated,
                                 @JsonProperty("items") List<ObjectLifecycleRule> items) {
        this.timeCreated = timeCreated;
        this.items = items;
    }

    public Instant getTimeCreated() {
        return timeCreated;
    }

    public List<ObjectLifecycleRule> getItems() {
        return items;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ObjectLifecyclePolicy)) return false;
        ObjectLifecyclePolicy that = (ObjectLifecyclePolicy) o;
        return Objects.equals(timeCreated, that.timeCreated) &&
                Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeCreated, items);
    }
}
