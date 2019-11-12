package com.oracle.pic.casper.webserver.api.model.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.common.model.BucketPublicAccessType;
import com.oracle.pic.casper.common.model.ObjectLevelAuditMode;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.webserver.api.common.NamespaceCaseWhiteList;
import com.oracle.pic.casper.webserver.api.model.BucketUpdate;
import com.oracle.pic.casper.webserver.api.model.exceptions.MissingBucketNameException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Helper methods to read a {@link BucketUpdate} structure from UTF-8 encoded JSON bytes.
 *
 * A bucket update has the following JSON representation:
 *
 * <code>
 *     {
 *         "namespace": optional string, the namespace in which the bucket is stored.
 *         "name": optional string, the name of the bucket.
 *         "metadata": dict with string keys and string values representing user defined metadata.
 *         "createdOn": a created on date that is not settable by clients, and is ignored by this code (but it is not
 *                      an error for a client to include it).
 *         "etag": the entity tag for the bucket, which is not settable by clients and is ignored by this code (but it
 *                 is not an error for a client to include it).
 *     }
 * </code>
 *
 * See the {@link BucketUpdate} class docs for details on what the individual fields mean and how they are used.
 */
public final class BucketUpdateJson {

    private BucketUpdateJson() {

    }

    /**
     * Read a {@link BucketUpdate} from a JSON string.
     *
     * @param namespace The namespace of the bucket, which is used if the bucket does not contain a "namespace" field.
     *                  Must be non-null. If there is a "namespace" field in the JSON, the returned BucketUpdate
     *                  contains that value instead of this one. This method does not validate that they are the same
     *                  value in that case.
     * @param bucketName The bucket name, which may not be null. If there is a "name" field, the value of that field is
     *                   returned in the BucketUpdate. This method does not validate that they are the same. If there is
     *                   no "name" field, this name is returned in the BucketUpdate.
     * @param bytes the JSON bytes (expected to be UTF-8 encoded characters).
     * @param mapper the Jackson object mapper used to parse the JSON.
     * @return the bucket update structure.
     * @throws InvalidBucketJsonException if anything about the JSON content was incorrect.
     */
    public static BucketUpdate bucketUpdateFromJson(
            String namespace, String bucketName, byte[] bytes, ObjectMapper mapper) {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(bytes);
        Preconditions.checkNotNull(mapper);

        final JsonNode root;
        try {
            root = mapper.readTree(bytes);
            if (root == null) {
                throw new IOException();
            }
        } catch (IOException ioex) {
            throw new InvalidBucketJsonException("The string '" + new String(bytes, Charsets.UTF_8) +
                    "' could not be parsed as a JSON object");
        }

        if (root.getNodeType() != JsonNodeType.OBJECT) {
            throw new InvalidBucketJsonException("The bucket update must be a JSON object");
        }

        String compartmentId = null;
        boolean isMissingMetadata = true;
        Map<String, String> metadata = null;
        Map<String, String> freeformTags = null;
        Map<String, Map<String, Object>> definedTags = null;
        BucketPublicAccessType bucketPublicAccessType = null;
        ObjectLevelAuditMode objectLevelAuditMode = null;
        String kmsKeyId = null;
        Boolean objectEventsEnabled = null;

        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = root.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();

            if (field.getKey().equals("compartmentId")) {
                BucketJsonCommon.checkUpdateBucketCompartmentIdNodeType(field.getValue().getNodeType());
            } else {
                BucketJsonCommon.checkNodeType(field.getKey(), field.getValue().getNodeType());
            }

            if (field.getKey().equals("namespace")) {
                // TODO: Delete overriding the namespace thru the body.
                // This behavior is not supported in the official SDK.
                namespace = field.getValue().asText();
                namespace = NamespaceCaseWhiteList.lowercaseNamespace(Api.V2, namespace);
            } else if (field.getKey().equals("compartmentId") && field.getValue().getNodeType() != JsonNodeType.NULL) {
                if (field.getValue().asText().length() == 0) {
                    throw  new InvalidBucketJsonException("The bucket movement must have a valid compartmentID");
                }
                compartmentId = field.getValue().asText();
            } else if (field.getKey().equals("name")) {
                bucketName = field.getValue().asText();
            } else if (field.getKey().equals("metadata")) {
                isMissingMetadata = false;
                metadata = BucketJsonCommon.readMetadata(field.getValue());
            } else if (field.getKey().equals("publicAccessType") &&
                    field.getValue().getNodeType() != JsonNodeType.NULL) {
                bucketPublicAccessType = BucketPublicAccessType.fromValue(field.getValue().asText());
            } else if (field.getKey().equals("objectLevelAuditMode") &&
                    field.getValue().getNodeType() != JsonNodeType.NULL) {
                objectLevelAuditMode = ObjectLevelAuditMode.fromValue(field.getValue().asText());
            } else if (field.getKey().equals("freeformTags")) {
                freeformTags = BucketJsonCommon.readFreeformTags(field.getValue());
            } else if (field.getKey().equals("definedTags")) {
                definedTags = (Map) BucketJsonCommon.readDefinedTags(field.getValue(), false);
            } else if (field.getKey().equals("kmsKeyId") && field.getValue().getNodeType() != JsonNodeType.NULL) {
                kmsKeyId = field.getValue().asText();
            } else if (field.getKey().equals("objectEventsEnabled")) {
                objectEventsEnabled = field.getValue().booleanValue();
            }
        }

        if (bucketName == null) {
            throw new MissingBucketNameException();
        }

        BucketUpdate.Builder builder = BucketUpdate.builder()
                .namespace(namespace)
                .compartmentId(compartmentId)
                .bucketName(bucketName)
                .metadata(metadata)
                .isMissingMetadata(isMissingMetadata)
                .bucketPublicAccessType(bucketPublicAccessType)
                .objectLevelAuditMode(objectLevelAuditMode)
                .freeformTags(freeformTags)
                .definedTags(definedTags)
                .kmsKeyId(kmsKeyId);

        if (objectEventsEnabled != null) {
            builder.objectEventsEnabled(objectEventsEnabled);
        }

        return builder.build();
    }
}
