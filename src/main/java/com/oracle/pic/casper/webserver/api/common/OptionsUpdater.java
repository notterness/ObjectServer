package com.oracle.pic.casper.webserver.api.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpServerRequest;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public final class OptionsUpdater {

    private OptionsUpdater() {
    }

    /**
     * Options are updated in the following way:
     * - if newOptions is null or empty, then return an empty map (all existing options are deleted)
     * - otherwise the returned map consists of the existing options with the following changes:
     *     any keys that exist in oldOptions but not newOptions are left unchanged
     *     keys in newOptions that have null values are removed from oldOptions,
     *     any other keys (new/existing) in newOptions are added or updated,
 *
     *     NOTE: {@link HttpContentHelpers#readUpdateBucketOptionsRequestJson(HttpServerRequest, ObjectMapper, byte[])}
     *     does not allow null to be passed as a value for the incoming option map. This method equates null
     *     to an empty map.
     */
    public static Map<String, Object> updateOptions(Map<String, Object> oldOptions,
                                                     @Nullable Map<String, Object> newOptions) {
        if (newOptions == null || newOptions.isEmpty()) {
            return new HashMap<>();
        } else {
            final Map<String, Object> mergedOptions =
                    oldOptions == null ?  new HashMap<>() : new HashMap<>(oldOptions);
            for (Map.Entry<String, Object> entry : newOptions.entrySet()) {
                if (entry.getValue() == null) {
                    mergedOptions.remove(entry.getKey());
                } else {
                    mergedOptions.put(entry.getKey(), entry.getValue());
                }
            }
            return mergedOptions;
        }

    }
}
