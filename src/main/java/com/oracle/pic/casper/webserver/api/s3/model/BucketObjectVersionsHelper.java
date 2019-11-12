package com.oracle.pic.casper.webserver.api.s3.model;

import com.google.common.base.Preconditions;

/**
 * Help with converting an object key-name to a version-id, and vice versa
 */
public final class BucketObjectVersionsHelper {
    public static String convertKeyNameToVersionId(String key) {
        Preconditions.checkNotNull(key, "Key should not be null");

        return "version_id_" + key;
    }

    public static String convertVersionIdToKeyName(String versionId) throws IllegalArgumentException {
        Preconditions.checkNotNull(versionId, "versionId should not be null");
        Preconditions.checkArgument(versionId.startsWith("version_id_"), "Invalid version_id");
        return versionId.replaceFirst("^version_id_", "");
    }

    private BucketObjectVersionsHelper() {
        throw new AssertionError("Instantiating a Utility class");
    }

}
