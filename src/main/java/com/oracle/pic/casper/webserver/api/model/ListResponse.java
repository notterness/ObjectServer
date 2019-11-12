package com.oracle.pic.casper.webserver.api.model;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class ListResponse<T extends ObjectBaseSummary> {
    private final List<? extends ObjectBaseSummary> objects;
    private final ObjectBaseSummary nextObject;

    public ListResponse(List<? extends ObjectBaseSummary> objects,
                 @Nullable ObjectBaseSummary nextObject) {
        this.objects = objects;
        this.nextObject = nextObject;
    }

    public List<? extends ObjectBaseSummary> getObjects() {
        return objects;
    }

    public Optional<ObjectBaseSummary> getNextObject() {
        return Optional.ofNullable(nextObject);
    }
}
