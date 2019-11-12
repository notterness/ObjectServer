package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * A representation of a paginated list with zero or more items and a flag indicating that there are more.
 *
 * @param <T> the type of the list of items.
 */
public class PaginatedList<T> {
    /**
     * A list of zero or more items.
     */
    private final List<T> items;

    /**
     * True if there are more items to be fetched, false otherwise. This is always false if items is empty.
     */
    private final boolean isTruncated;

    public PaginatedList(@JsonProperty("keys") Iterable<T> items,
                         @JsonProperty("truncated") boolean isTruncated) {
        this.items = ImmutableList.copyOf(items);
        Preconditions.checkArgument(!isTruncated || !this.items.isEmpty());
        this.isTruncated = isTruncated;
    }

    public List<T> getItems() {
        return items;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaginatedList<?> that = (PaginatedList<?>) o;
        return isTruncated() == that.isTruncated() &&
                Objects.equals(getItems(), that.getItems());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getItems(), isTruncated());
    }

    @Override
    public String toString() {
        return "PaginatedList{" +
                "items=" + items +
                ", isTruncated=" + isTruncated +
                '}';
    }
}
