package com.oracle.pic.casper.webserver.api.swift;

/**
 * Headers used only by Swift.
 */
public final class SwiftHeaders {

    public static final String TIMESTAMP = "X-Timestamp";
    public static final String TRANS_ID = "X-Trans-Id";
    public static final String STORAGE_POLICY = "X-Storage-Policy";
    public static final String ACCOUNT_CONTAINER_COUNT = "X-Account-Container-Count";
    public static final String ACCOUNT_BYTES_USED = "X-Account-Bytes-Used";
    public static final String CONTAINER_OBJECT_COUNT = "X-Container-Object-Count";
    public static final String CONTAINER_BYTES_USED = "X-Container-Bytes-Used";
    public static final String COMPARTMENT_ID_HEADER = "X-Container-Meta-Compartment-Id";

    public static final String RM_CONTAINER_META_PREFIX = "X-Remove-Container-Meta-";

    public static final String LARGE_OBJECT_HEADER = "X-Object-Manifest";

    private SwiftHeaders() {
    }
}
