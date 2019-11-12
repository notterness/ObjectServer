package com.oracle.pic.casper.webserver.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * A representation of a paginated list with zero or more items and a String the next page should start after.
 *
 * @param <T> the type of the list of items.
 */
public class ParPaginatedList<T> {
    /**
     * A list of zero or more items.
     */
    private final List<T> items;

    /**
     * The token that the next page should start after. null if we know there is no more page or this page is empty
     */
    private final String nextPageToken;

    public ParPaginatedList(@JsonProperty("keys") Iterable<T> items,
                            @JsonProperty("nextPageToken") String nextPageToken) {
        this.items = ImmutableList.copyOf(items);
        Preconditions.checkArgument(nextPageToken == null || !this.items.isEmpty());
        this.nextPageToken = nextPageToken;
    }

    public List<T> getItems() {
        return items;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParPaginatedList<?> that = (ParPaginatedList<?>) o;
        return Objects.equals(getNextPageToken(), that.getNextPageToken()) &&
                Objects.equals(getItems(), that.getItems());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getItems(), getNextPageToken());
    }

    @Override
    public String toString() {
        return "PaginatedList{" +
                "items=" + items +
                ", nextPageToken=" + nextPageToken +
                '}';
    }
}
