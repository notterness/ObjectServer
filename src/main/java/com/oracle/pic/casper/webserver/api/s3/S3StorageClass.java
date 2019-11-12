package com.oracle.pic.casper.webserver.api.s3;

import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;

public enum S3StorageClass {
    STANDARD, GLACIER;

    public static S3StorageClass valueFrom(ObjectMetadata objectMetadata) {
        return objectMetadata.getArchivalState() == ArchivalState.Archived ? GLACIER : STANDARD;
    }
}
