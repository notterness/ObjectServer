package com.oracle.pic.casper.webserver.auth;

import javax.annotation.Nullable;

/**
 * A enumeration of permission names that are supported by the Casper service, see
 * com.oracle.pic.casper.webserver.api.auth.Authorizer for how these are used.
 *
 * The full set of permissions, and their meaning, is documented here:
 *  https://confluence.oci.oraclecorp.com/display/ID/Permissions+of+Casper
 */
public enum CasperPermission {
    OBJECTSTORAGE_NAMESPACE_READ(CasperResourceKind.NAMESPACES),
    OBJECTSTORAGE_NAMESPACE_UPDATE(CasperResourceKind.NAMESPACES),
    //This needs to be objectstorage-namespaces. Identity needs to be notified to create new Metaverb mapping
    //TODO : Create Jira and add link here.
    TENANCY_INSPECT(CasperResourceKind.TENANCIES),

    OBJECT_CREATE(CasperResourceKind.OBJECTS),
    OBJECT_READ(CasperResourceKind.OBJECTS),
    OBJECT_INSPECT(CasperResourceKind.OBJECTS),
    OBJECT_DELETE(CasperResourceKind.OBJECTS),
    OBJECT_OVERWRITE(CasperResourceKind.OBJECTS),
    OBJECT_RESTORE(CasperResourceKind.OBJECTS),

    BUCKET_CREATE(CasperResourceKind.BUCKETS),
    BUCKET_UPDATE(CasperResourceKind.BUCKETS),
    BUCKET_READ(CasperResourceKind.BUCKETS),
    BUCKET_INSPECT(CasperResourceKind.BUCKETS),
    BUCKET_DELETE(CasperResourceKind.BUCKETS),

    PAR_MANAGE(CasperResourceKind.BUCKETS),

    BUCKET_REPLICATION_MANAGE(CasperResourceKind.BUCKETS),

    CHECK_ALL_OBJECTS(CasperResourceKind.OPERATOR),

    BUCKET_PURGE_TAGS(CasperResourceKind.BUCKETS),

    INTERNAL_CP_USAGE_READ(CasperResourceKind.NAMESPACES);

    /**
     * Resource kind is a meta mapping of permissions for Identity i.e. `allow user to manage objects in tenancy`
     * The mapping of each permission to resource kind can be found:
     * https://confluence.oci.oraclecorp.com/display/PLAT/Permissions+of+Casper
     * https://confluence.oci.oraclecorp.com/display/PLAT/Verb+Permissions+for+GA
     */
    private final CasperResourceKind resourceKind;

    CasperPermission(CasperResourceKind resourceKind) {
        this.resourceKind = resourceKind;
    }

    @Nullable
    public CasperResourceKind getResourceKind() {
        return resourceKind;
    }
}
