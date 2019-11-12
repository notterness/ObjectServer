package com.oracle.pic.casper.webserver.api.common;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public final class MetadataUpdater {

    private MetadataUpdater() {
    }

    /**
     * Metadata is updated in the following way:
     * - if isMissingNewMetadata is true, return oldMetadata
     * - if newMetatdata is null, then return empty map
     * - otherwise, keys in oldMetadata that have null values in newMetadata are removed,
     *   any other keys in newMetadata is added or updated,
     *   and any keys that exist in oldMetadata but not newMetadata are left unchanged.
     */
    public static Map<String, String> updateMetadata(boolean isMissingNewMetadata,
                                                     Map<String, String> oldMetadata,
                                                     @Nullable Map<String, String> newMetadata) {
        if (isMissingNewMetadata) {
            return oldMetadata;
        } else if (newMetadata == null) {
            return new HashMap<>();
        } else {
            final Map<String, String> mergedMetadata = oldMetadata == null ?
                new HashMap<>() :
                new HashMap<>(oldMetadata);
            for (Map.Entry<String, String> entry : newMetadata.entrySet()) {
                if (entry.getValue() == null) {
                    mergedMetadata.remove(entry.getKey());
                } else {
                    mergedMetadata.put(entry.getKey(), entry.getValue());
                }
            }
            return mergedMetadata;
        }

    }
}
