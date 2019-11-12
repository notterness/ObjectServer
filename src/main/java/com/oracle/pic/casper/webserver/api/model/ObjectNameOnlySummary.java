package com.oracle.pic.casper.webserver.api.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;

import java.util.List;
import java.util.Objects;

public class ObjectNameOnlySummary implements ObjectBaseSummary {

    private final String objectName;

    public ObjectNameOnlySummary(String objectName) {
        this.objectName = Preconditions.checkNotNull(objectName);
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public ObjectSummary makeSummary(List<ObjectProperties> properties, DecidingKeyManagementService kms) {
        ObjectSummary objectSummary =  new ObjectSummary();
        for (ObjectProperties op : properties) {
            switch (op) {
                case NAME:
                    objectSummary.setName(this.objectName);
                    break;
                default:
                    // We pre-validate the object properties for name-only lists, so if we don't recognize a property,
                    // fail fast because something is wrong.
                    throw new IllegalArgumentException(
                            "Object Property '" + op + "' not valid for ObjectNameOnlySummary");
            }
        }
        return objectSummary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectNameOnlySummary that = (ObjectNameOnlySummary) o;
        return Objects.equals(objectName, that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("objectName", objectName)
                .toString();
    }
}
