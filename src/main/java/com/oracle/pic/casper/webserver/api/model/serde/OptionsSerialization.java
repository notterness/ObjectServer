package com.oracle.pic.casper.webserver.api.model.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 * Methods to serialize and deserialize object storage bucket options.
 */
public final class OptionsSerialization {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(OptionsSerialization.class);

    /**
     * Jackson type for Map<String, Object> serialization.
     */
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
    };

    /**
     * Object reader used to deserialize a Map<String, Object>.
     */
    private static final ObjectWriter MAP_WRITER = new ObjectMapper().writerFor(MAP_TYPE);

    /**
     * Object reader used to deserialize a Map<String, Object>.
     */
    private static final ObjectReader MAP_READER = new ObjectMapper().readerFor(MAP_TYPE);

    /**
     * Private constructor for static class.
     */
    private OptionsSerialization() {
    }

    /**
     * Serialize bucket options to a JSON string.
     *
     * @param options The options to serialize, which may be null (in which case an empty JSON map is serialized).
     * @return Serialized string.
     */
    public static String serializeOptions(@Nullable Map<String, Object> options) {
        if (options == null) {
            options = ImmutableMap.of();
        }

        try {
            return MAP_WRITER.writeValueAsString(options);
        } catch (JsonProcessingException ex) {
            LOG.error("Error serializing \"" + options + "\"", ex);
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Deserialize a JSON-formatted string representing the bucket options.
     *
     * @param serializedOptions The serialized options, which may be null (in which case an empty JSON map is
     *                          returned).
     * @return A {@link Map<String, Object>} containing all the key/value options
     */
    public static Map<String, Object> deserializeOptions(@Nullable String serializedOptions) {
        if (serializedOptions == null) {
            return ImmutableMap.of();
        }
        try {
            return MAP_READER.readValue(serializedOptions);
        } catch (IOException ex) {
            LOG.error("Error deserializing \"" + serializedOptions + "\"");
            throw new IllegalStateException(serializedOptions, ex);
        }
    }
}
