package com.oracle.pic.casper.webserver.limit;

public enum ResourceType {

    IDENTITY("Identity"),
    KMS("Kms"),
    LIMITS("Limits"),
    OPERATOR_MDS_SERVICE("OperatorMdsService"),
    WORK_REQUEST_MDS_SERVICE("WorkRequestMdsService"),
    TENANT_MDS_SERVICE("TenantMdsService"),
    OBJECT_MDS_SERVICE("ObjectMdsService"),
    VOLUME_SERVICE("VolumeService");

    private final String name;

    ResourceType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ResourceType getEnumValue(String name) {
        for (ResourceType resourceType : ResourceType.values()) {
            if (resourceType.getName().equals(name)) {
                return resourceType;
            }
        }
        throw new IllegalArgumentException(String.format("Invalid resource type name %s.", name));
    }
}
