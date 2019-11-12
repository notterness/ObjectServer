package com.oracle.pic.casper.webserver.util;

import com.oracle.pic.casper.webserver.api.model.BucketLoggingStatus;
import com.oracle.pic.casper.webserver.api.model.BucketOptions;
import com.oracle.pic.casper.webserver.api.model.serde.OptionsSerialization;

import java.util.Map;

/**
 * Helper class to get {@link BucketLoggingStatus} from the BucketOptions
 */
public final class BucketOptionsUtil {

    private BucketOptionsUtil() {

    }

    public static BucketLoggingStatus getBucketLoggingStatusFromOptions(String serializedOptions) {
        return  getBucketLoggingStatusFromOptions(OptionsSerialization.deserializeOptions(serializedOptions));
    }

    public static BucketLoggingStatus getBucketLoggingStatusFromOptions(Map<String, Object> deserializedOptions) {
        String readLogId = (String) deserializedOptions.getOrDefault(BucketOptions.READ_LOG_ID.getValue(), null);
        String writeLogId = (String) deserializedOptions.getOrDefault(BucketOptions.WRITE_LOG_ID.getValue(), null);
        return new BucketLoggingStatus.BucketLoggingStatusBuilder().readLogId(readLogId).writeLogId(writeLogId).build();
    }
}
