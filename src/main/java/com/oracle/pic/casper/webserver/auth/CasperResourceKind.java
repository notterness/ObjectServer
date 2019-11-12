package com.oracle.pic.casper.webserver.auth;

public enum CasperResourceKind {

    NAMESPACES("objectstorage-namespaces"),
    BUCKETS("buckets"),
    OBJECTS("objects"),
    OBJECT_FAMILY("object-family"),
    TENANCIES("tenancies"),
    OPERATOR("operator");

    private final String name;

    CasperResourceKind(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
