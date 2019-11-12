package com.oracle.pic.casper.webserver.api.model.swift;

public class ContainerAndObject {
    private final String container;
    private final String object;

    public ContainerAndObject(String container, String object) {
        this.container = container;
        this.object = object;
    }

    public String getContainer() {
        return container;
    }

    public String getObject() {
        return object;
    }

    @Override
    public String toString() {
        return container + '/' + object;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ContainerAndObject that = (ContainerAndObject) o;

        if (!container.equals(that.container)) return false;
        return object.equals(that.object);
    }

    @Override
    public int hashCode() {
        int result = container.hashCode();
        result = 31 * result + object.hashCode();
        return result;
    }
}
