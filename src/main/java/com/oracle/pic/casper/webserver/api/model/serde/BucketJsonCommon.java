package com.oracle.pic.casper.webserver.api.model.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Helper methods for deserializing {@link com.oracle.pic.casper.webserver.api.model.BucketUpdate} and
 * {@link com.oracle.pic.casper.webserver.api.model.BucketCreate} objects.
 *
 * These two objects share many fields in common, so this class defines those fields and their allowable values.
 */
public final class BucketJsonCommon {
    // Mapping from field names to the JSON type we expect for them.
    static final Map<String, JsonNodeType> KNOWN_FIELDS = ImmutableMap.<String, JsonNodeType>builder()
            .put("namespace", JsonNodeType.STRING)
            .put("name", JsonNodeType.STRING)
            .put("compartmentId", JsonNodeType.STRING)
            .put("createdBy", JsonNodeType.STRING)
            .put("createdOn", JsonNodeType.STRING)
            .put("metadata", JsonNodeType.OBJECT)
            .put("etag", JsonNodeType.STRING)
            .put("publicAccessType", JsonNodeType.STRING)
            .put("storageTier", JsonNodeType.STRING)
            .put("objectLevelAuditMode", JsonNodeType.STRING)
            .put("freeformTags", JsonNodeType.OBJECT)
            .put("definedTags", JsonNodeType.OBJECT)
            .put("kmsKeyId", JsonNodeType.STRING)
            .put("objectEventsEnabled", JsonNodeType.BOOLEAN)
            .build();

    // Fields that are allowed to be explicitly set to null in the JSON object.
    private static final Set<String> NULLABLE_FIELDS = ImmutableSet.of("metadata",
            "publicAccessType", "storageTier", "objectLevelAuditMode", "freeformTags", "definedTags",
            "objectLifecyclePolicyEtag", "kmsKeyId");

    private BucketJsonCommon() {

    }

    /**
     * Check that the given field name has the correct field type (or null, if that is allowed).
     *
     * @param fieldName the name of the field in the JSON object.
     * @param actual the JSON type of the field's value in the JSON object.
     */
    static void checkNodeType(String fieldName, JsonNodeType actual) {
        if (!KNOWN_FIELDS.containsKey(fieldName)) {
            throw new InvalidBucketJsonException("The field '" + fieldName + "' is unknown");
        }

        if (actual == JsonNodeType.NULL) {
            if (!NULLABLE_FIELDS.contains(fieldName)) {
                throw new InvalidBucketJsonException("The value of the field '" + fieldName + "' must not be null");
            }
        } else {
            JsonNodeType expected = KNOWN_FIELDS.get(fieldName);
            if (actual != expected) {
                throw new InvalidBucketJsonException(
                        "The field '" + fieldName + "' must have a JSON value of type '" + expected +
                                "' but it had a value of type '" + actual + "'");
            }
        }
    }

    /**
     * Special for checking compartmentID field in UpdateBucketRequest. However, it isn't able to tell whether NULL
     * compartmentID is set by SDK as default value or by client on purpose.
     */
    static void checkUpdateBucketCompartmentIdNodeType(JsonNodeType actual) {
        if (actual != JsonNodeType.NULL && actual != KNOWN_FIELDS.get("compartmentId")) {
            throw new InvalidBucketJsonException(
                    "The field 'compartmentId' must have a JSON value of type 'String' " +
                            "but it had a value of type '" + actual + "'");
        }
    }

    /**
     * Builds a metadata map from the metadata JSON node.  Metadata keys are converted to lowercase.
     */
    static Map<String, String> readMetadata(JsonNode metadataObjNode) {
        return readValStringMap(metadataObjNode, "metadata", true);
    }

    static Map<String, String> readFreeformTags(JsonNode freeformTagsObjNode) {
        return readValStringMap(freeformTagsObjNode, "freeformTags", false);
    }

    static Map<String, Map<String, Object>> readDefinedTags(JsonNode definedTagsObjNode, boolean isKeyCaseInsensitive) {
        if (definedTagsObjNode.getNodeType() == JsonNodeType.NULL) {
            return null;
        }

        if (definedTagsObjNode.getNodeType() != JsonNodeType.OBJECT) {
            throw new InvalidBucketJsonException("The value of the 'definedTags' field must be a JSON object.");
        }

        Map<String, Map<String, Object>> definedTagsMap = Maps.newHashMap();
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = definedTagsObjNode.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            String key = field.getKey();
            if (isKeyCaseInsensitive) {
                key = key.toLowerCase();
            }
            JsonNode value = field.getValue();
            switch (value.getNodeType()) {
                case ARRAY:
                case NUMBER:
                case BOOLEAN:
                case STRING:
                    throw new InvalidBucketJsonException(
                            String.format("The value of the '%s' entry in the 'definedTags' " +
                                            "field must be a JSON object (it was a '%s').",
                                    key, value.getNodeType().name()));
                case OBJECT:
                    definedTagsMap.put(key, readValPrimitiveMap(value, "definedTags." + key,
                            isKeyCaseInsensitive));
                    break;
                case NULL:
                    definedTagsMap.put(key, null);
                    break;
                default:
                    throw new InvalidBucketJsonException(
                            String.format("The value of the '%s' entry in the 'definedTags' field was not recognized.",
                                    key));
            }
        }
        return definedTagsMap;
    }

    static Map<String, String> readValStringMap(JsonNode objNode, String bucketField, boolean isKeyCaseInsensitive) {
        if (objNode.getNodeType() == JsonNodeType.NULL) {
            return null;
        }

        if (objNode.getNodeType() != JsonNodeType.OBJECT) {
            throw new InvalidBucketJsonException(String.format("The value of the '%s' field must be a JSON object.",
                    bucketField));
        }

        Map<String, String> map = Maps.newHashMap();
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = objNode.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            String key = field.getKey();
            if (isKeyCaseInsensitive) {
                key = key.toLowerCase();
            }
            JsonNode value = field.getValue();

            switch (value.getNodeType()) {
                case ARRAY:
                case OBJECT:
                case NUMBER:
                case BOOLEAN:
                    throw new InvalidBucketJsonException(
                            String.format("The value of the '%s' entry in the '%s' field must be a JSON string " +
                                            "(it was a '%s').",
                                    key, bucketField, value.getNodeType().name()));
                case NULL:
                    map.put(key, null);
                    break;
                case STRING:
                    map.put(key, value.asText());
                    break;
                default:
                    throw new InvalidBucketJsonException(
                            String.format("The value of the '%s' entry in the '%s' field was not recognized.",
                                    key, bucketField));
            }
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * This method is only used to parse definedTags values, we DON'T validate data types, we just pass all of
     * them to tagging. In GA Tagging only support string as the value, all the other types are invalid, and the
     * validation should happen in Tagging. For now we pares primitive types and leave the other types as JsonNode
     * OBJECT, ARRAY, POJO, BINARY, MISSING and pass them to tagging.
     */
    static Map<String, Object> readValPrimitiveMap(JsonNode objNode, String bucketField, boolean isKeyCaseInsensitive) {
        if (objNode.getNodeType() == JsonNodeType.NULL) {
            return null;
        }

        if (objNode.getNodeType() != JsonNodeType.OBJECT) {
            throw new InvalidBucketJsonException(String.format("The value of the '%s' field must be a JSON object.",
                    bucketField));
        }

        Map<String, Object> map = Maps.newHashMap();
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = objNode.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            String key = field.getKey();
            if (isKeyCaseInsensitive) {
                key = key.toLowerCase();
            }
            JsonNode value = field.getValue();

            switch (value.getNodeType()) {
                case NUMBER:
                    map.put(key, value.numberValue());
                    break;
                case BOOLEAN:
                    map.put(key, value.booleanValue());
                    break;
                case STRING:
                    map.put(key, value.asText());
                    break;
                case NULL:
                    map.put(key, null);
                    break;
                case OBJECT:
                case ARRAY:
                case MISSING:
                case POJO:
                case BINARY:
                    map.put(key, value);
                    break;
                default:
                    throw new InvalidBucketJsonException(
                            String.format("The value of the '%s' entry in the '%s' field was not recognized.",
                                    key, bucketField));
            }
        }

        return Collections.unmodifiableMap(map);
    }
}
