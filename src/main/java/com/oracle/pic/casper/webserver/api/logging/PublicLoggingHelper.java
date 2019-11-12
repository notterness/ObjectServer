package com.oracle.pic.casper.webserver.api.logging;

import com.oracle.pic.casper.common.exceptions.BadRequestException;
import com.oracle.pic.casper.webserver.api.model.BucketOptions;

public final class PublicLoggingHelper {

    private PublicLoggingHelper() {
    }

    public static String getBucketOptionFromCategory(String category) {
        switch (category.toUpperCase()) {
            case "READ" :
                return BucketOptions.READ_LOG_ID.getValue();
            case "WRITE" :
                return BucketOptions.WRITE_LOG_ID.getValue();
            default :
                throw new BadRequestException("Unrecognized category value: " + category);
        }
    }
}
